#!/usr/bin/env python3
"""
MHBD Cloud Transcoder Worker - Runs inside GitHub Actions Runner.
Directly downloads source from Google Drive, transcodes 720p/480p with ultrafast,
sends live progress callbacks to VPS, and uploads encoded streams directly to Drive.
"""
import os
import sys
import json
import time
import re
import math
import subprocess
import requests

def send_callback(callback_url, secret, payload):
    if not callback_url:
        return
    headers = {
        "Content-Type": "application/json",
        "X-Pipeline-Secret": secret,
        "Authorization": f"Bearer {secret}"
    }
    try:
        r = requests.post(callback_url, json=payload, headers=headers, timeout=15)
        print(f"[Callback] {payload.get('action')} -> HTTP {r.status_code}")
    except Exception as e:
        print(f"[Callback Warning] Failed to send callback: {e}")

def refresh_drive_token(client_id, client_secret, refresh_token):
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def download_file(url, target_path, headers=None):
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        t0 = time.time()
        last_log = t0
        with open(target_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if now - last_log > 3:
                        pct = (downloaded / total_size * 100) if total_size > 0 else 0
                        speed = (downloaded / (now - t0)) / (1024 * 1024)
                        print(f"   Downloading: {downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB ({pct:.1f}%) @ {speed:.1f} MB/s")
                        last_log = now

def get_drive_offset(upload_url, file_size):
    """Query Google Drive to find out how many bytes were safely received."""
    for attempt in range(5):
        try:
            r = requests.put(
                upload_url,
                headers={"Content-Range": f"bytes */{file_size}"},
                timeout=30
            )
            if r.status_code in (200, 201):
                return file_size, r.json()
            if r.status_code == 308:
                range_header = r.headers.get("Range")
                if range_header:
                    last_byte = int(range_header.split("-")[1])
                    return last_byte + 1, None
                return 0, None
        except Exception as e:
            print(f"      [Status Query Attempt {attempt+1}] Error querying upload status: {e}")
            time.sleep(2 * (attempt + 1))
    return 0, None

def upload_to_drive(file_path, folder_id, access_token, mime_type="application/octet-stream"):
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    print(f"[Drive Upload] Uploading {file_name} ({file_size / (1024*1024):.2f} MB)...")

    meta = {
        "name": file_name,
        "parents": [folder_id] if folder_id else []
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": mime_type,
        "X-Upload-Content-Length": str(file_size)
    }

    upload_url = None
    for init_try in range(5):
        try:
            init_res = requests.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
                headers=headers,
                json=meta,
                timeout=45
            )
            init_res.raise_for_status()
            upload_url = init_res.headers.get("Location")
            if upload_url:
                break
        except Exception as e:
            print(f"[Drive Upload] Init try {init_try+1} failed: {e}. Retrying in 3s...")
            time.sleep(3)

    if not upload_url:
        raise RuntimeError(f"Could not initiate Google Drive resumable upload session for {file_name}")

    chunk_size = 32 * 1024 * 1024
    with open(file_path, "rb") as f:
        offset = 0
        last_log_time = time.time()
        while offset < file_size:
            f.seek(offset)
            chunk = f.read(chunk_size)
            chunk_len = len(chunk)
            up_headers = {
                "Content-Length": str(chunk_len),
                "Content-Range": f"bytes {offset}-{offset + chunk_len - 1}/{file_size}"
            }

            chunk_success = False
            for retry in range(7):
                try:
                    r = requests.put(upload_url, headers=up_headers, data=chunk, timeout=180)
                    if r.status_code in (200, 201):
                        res_data = r.json()
                        print(f"[Drive Upload] Done: {file_name} -> ID: {res_data.get('id')}")
                        return res_data.get("id"), file_size
                    elif r.status_code == 308:
                        offset += chunk_len
                        chunk_success = True
                        now = time.time()
                        if now - last_log_time > 10 or offset >= file_size:
                            pct = (offset / file_size) * 100
                            print(f"   [Drive Upload] {offset / (1024*1024):.1f} MB / {file_size / (1024*1024):.1f} MB ({pct:.1f}%)")
                            last_log_time = now
                        break
                    elif r.status_code in (500, 502, 503, 504, 408):
                        print(f"   [Drive Upload] Server returned HTTP {r.status_code}, checking status in {2*(retry+1)}s...")
                        time.sleep(2 * (retry + 1))
                        server_offset, final_data = get_drive_offset(upload_url, file_size)
                        if final_data and (final_data.get("id")):
                            print(f"[Drive Upload] Done (recovered): {file_name} -> ID: {final_data.get('id')}")
                            return final_data.get("id"), file_size
                        if server_offset > offset:
                            offset = server_offset
                            chunk_success = True
                            break
                    else:
                        print(f"   [Drive Upload] Unexpected HTTP {r.status_code}: {r.text[:200]}")
                        time.sleep(3)
                except Exception as err:
                    print(f"   [Drive Upload] Network/SSL error at byte {offset} ({err}), retry {retry+1}/7...")
                    time.sleep(3 * (retry + 1))
                    server_offset, final_data = get_drive_offset(upload_url, file_size)
                    if final_data and final_data.get("id"):
                        print(f"[Drive Upload] Done (recovered after SSL drop): {file_name} -> ID: {final_data.get('id')}")
                        return final_data.get("id"), file_size
                    if server_offset > offset:
                        offset = server_offset
                        chunk_success = True
                        break

            if not chunk_success and offset < file_size:
                server_offset, final_data = get_drive_offset(upload_url, file_size)
                if final_data and final_data.get("id"):
                    return final_data.get("id"), file_size
                if server_offset > offset:
                    offset = server_offset
                    continue
                raise RuntimeError(f"Upload failed for {file_name} at byte offset {offset} after multiple retries")

    raise RuntimeError(f"Upload loop exited without completion for {file_name}")

def main():
    raw_payload = os.environ.get("EVENT_PAYLOAD", "{}")
    print(f"Payload received: {raw_payload[:200]}...")
    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    except Exception as e:
        print(f"Error parsing EVENT_PAYLOAD: {e}")
        sys.exit(1)

    video_id = payload.get("video_id") or payload.get("videoId")
    targets = payload.get("targets") or ["720p"]
    callback_url = payload.get("callback_url") or payload.get("callbackUrl")
    secret = payload.get("secret") or payload.get("pipeline_secret") or ""
    worker_id = payload.get("worker_id")
    worker_name = payload.get("worker_name") or "GitHub Cloud Worker"
    worker_repo = payload.get("worker_repo") or os.environ.get("GITHUB_REPOSITORY", "")

    drive_config = payload.get("drive_config") or {}
    client_id = drive_config.get("client_id")
    client_secret = drive_config.get("client_secret")
    refresh_token = drive_config.get("refresh_token")
    folder_id = drive_config.get("folder_id")
    drive_account_id = drive_config.get("drive_account_id")

    if not video_id:
        print("Missing video_id!")
        sys.exit(1)

    print(f"=== Starting Cloud Transcoding for Video: {video_id} ===")
    print(f"Worker: {worker_name} ({worker_repo})")
    print(f"Target resolutions: {targets}")

    # Notify progress: starting download
    send_callback(callback_url, secret, {
        "videoId": video_id,
        "workerId": worker_id,
        "workerName": worker_name,
        "workerRepo": worker_repo,
        "quality": targets[0],
        "action": "progress",
        "progressPct": 2,
        "speed": "Fetching source"
    })

    work_dir = os.path.join(os.getcwd(), "work")
    os.makedirs(work_dir, exist_ok=True)
    input_path = os.path.join(work_dir, "input.mp4")

    try:
        access_token = refresh_drive_token(client_id, client_secret, refresh_token) if refresh_token else None

        # 1. Download source from Google Drive
        gdrive_file_id = payload.get("gdrive_file_id") or payload.get("gdriveFileId")
        if gdrive_file_id and access_token:
            print(f"Fetching source from Google Drive (fileId: {gdrive_file_id})...")
            dl_url = f"https://www.googleapis.com/drive/v3/files/{gdrive_file_id}?alt=media"
            headers = {"Authorization": f"Bearer {access_token}"}
            download_file(dl_url, input_path, headers=headers)
        elif payload.get("source_url") or payload.get("sourceUrl"):
            src_url = payload.get("source_url") or payload.get("sourceUrl")
            print(f"Fetching source from direct URL...")
            download_file(src_url, input_path)
        else:
            raise RuntimeError("No valid source file ID or URL provided!")

        # Probe video duration & resolution
        probe_cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", input_path
        ]
        probe_res = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
        probe_data = json.loads(probe_res.stdout)
        duration_sec = float(probe_data.get("format", {}).get("duration", 0))
        if duration_sec <= 0:
            for st in probe_data.get("streams", []):
                if st.get("codec_type") == "video" and st.get("duration"):
                    duration_sec = float(st.get("duration"))
                    break
        total_duration = max(duration_sec, 1.0)
        print(f"Source probed: duration={total_duration:.1f}s")

        # 2. Transcode each target quality
        stream_dir = os.path.join(work_dir, "stream")
        os.makedirs(stream_dir, exist_ok=True)

        specs = {
            "720p": {"height": 720, "bitrate": 1400, "maxrate": 1680, "bufsize": 2800},
            "480p": {"height": 480, "bitrate": 700, "maxrate": 840, "bufsize": 1400},
            "360p": {"height": 360, "bitrate": 400, "maxrate": 480, "bufsize": 800},
        }

        for quality in targets:
            spec = specs.get(quality, specs["720p"])
            h = spec["height"]
            br = spec["bitrate"]
            out_m3u8 = os.path.join(stream_dir, f"video_{quality}.m3u8")

            print(f"\n--- Transcoding {quality} ({h}p @ {br}k) with ultrafast ---")
            ffmpeg_cmd = [
                "ffmpeg", "-y", "-err_detect", "ignore_err",
                "-fflags", "+genpts+discardcorrupt",
                "-i", input_path,
                "-map", "0:v:0", "-an",
                "-hls_time", "6",
                "-hls_playlist_type", "vod",
                "-hls_segment_type", "fmp4",
                "-hls_flags", "single_file",
                "-vf", f"scale=-2:{h}",
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-threads", "0",
                "-b:v", f"{br}k",
                "-maxrate", f"{spec['maxrate']}k",
                "-bufsize", f"{spec['bufsize']}k",
                "-g", "150", "-keyint_min", "150",
                "-sc_threshold", "0",
                "-f", "hls",
                out_m3u8
            ]

            p = subprocess.Popen(
                ffmpeg_cmd,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )

            last_callback_time = time.time()
            time_regex = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")
            speed_regex = re.compile(r"speed=\s*([\d\.]+)x")

            for line in p.stderr:
                m_time = time_regex.search(line)
                if m_time:
                    cur_secs = int(m_time.group(1))*3600 + int(m_time.group(2))*60 + float(m_time.group(3))
                    pct = min(99, max(5, int((cur_secs / total_duration) * 100)))

                    now = time.time()
                    if now - last_callback_time >= 5:
                        m_spd = speed_regex.search(line)
                        speed_str = f"{m_spd.group(1)}x" if m_spd else "1.0x"
                        speed_val = float(m_spd.group(1)) if (m_spd and float(m_spd.group(1)) > 0) else 1.0
                        rem_sec = max(0, int((total_duration - cur_secs) / speed_val))

                        print(f"   [{quality}] {pct}% (speed: {speed_str}, eta: ~{rem_sec}s)")
                        send_callback(callback_url, secret, {
                            "videoId": video_id,
                            "workerId": worker_id,
                            "workerName": worker_name,
                            "workerRepo": worker_repo,
                            "quality": quality,
                            "action": "progress",
                            "progressPct": pct,
                            "speed": speed_str,
                            "etaSeconds": rem_sec
                        })
                        last_callback_time = now

            p.wait()
            if p.returncode != 0:
                raise RuntimeError(f"FFmpeg exited with code {p.returncode} during {quality} encode")

            out_m4s = os.path.join(stream_dir, f"video_{quality}.m4s")
            if not os.path.exists(out_m3u8) or not os.path.exists(out_m4s):
                raise RuntimeError(f"Output files missing for {quality}")

            print(f"   {quality} transcode finished! Uploading to Google Drive...")

            # Re-fresh token if needed
            if refresh_token:
                access_token = refresh_drive_token(client_id, client_secret, refresh_token)

            # Upload m3u8 and m4s
            m3u8_id, m3u8_sz = upload_to_drive(out_m3u8, folder_id, access_token, mime_type="application/vnd.apple.mpegurl")
            m4s_id, m4s_sz = upload_to_drive(out_m4s, folder_id, access_token, mime_type="video/iso.segment")

            # Report completion for this quality to VPS
            send_callback(callback_url, secret, {
                "videoId": video_id,
                "workerId": worker_id,
                "workerName": worker_name,
                "workerRepo": worker_repo,
                "quality": quality,
                "action": "complete",
                "copies": [
                    {
                        "driveAccountId": drive_account_id,
                        "gdriveFileId": m3u8_id,
                        "fileSize": m3u8_sz,
                        "note": f"video_{quality}.m3u8"
                    },
                    {
                        "driveAccountId": drive_account_id,
                        "gdriveFileId": m4s_id,
                        "fileSize": m4s_sz,
                        "note": f"video_{quality}.m4s"
                    }
                ]
            })
            print(f"=== Quality {quality} successfully transcoded & registered! ===")

    except Exception as e:
        err_msg = str(e)
        print(f"\n[ERROR] Transcoding failed: {err_msg}")
        send_callback(callback_url, secret, {
            "videoId": video_id,
            "workerId": worker_id,
            "workerName": worker_name,
            "workerRepo": worker_repo,
            "quality": targets[0] if targets else "720p",
            "action": "fail",
            "errorMessage": err_msg[:1000]
        })
        sys.exit(1)

if __name__ == "__main__":
    main()
