#!/usr/bin/env python3
"""
MHBD Universal Video Compressor & Google Drive Pipeline Worker
- Runs on GitHub Actions Runner (ubuntu-latest)
- Multi-resolution compression (1080p -> 720p, 480p, 360p)
- Preserves all audio tracks, language metadata, and subtitles
- Smart dynamic filename resolution renaming (1080p -> 720p / 480p)
- Resumable Google Drive upload with folder management & shareable links
- Live progress callbacks to MHBD Streaming Next.js API
- ZERO hardcoded secrets: all credentials/tokens passed via runtime client_payload
"""

import os
import sys
import re
import json
import time
import shutil
import urllib.request
import urllib.parse
import subprocess
import requests

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def callback(url, secret, payload):
    if not url:
        return
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {secret}",
            "x-pipeline-secret": str(secret)
        }
        res = requests.post(url, json=payload, headers=headers, timeout=25)
        log(f"Callback sent ({payload.get('stage')}, {payload.get('progress')}%) -> HTTP {res.status_code}")
    except Exception as e:
        log(f"Callback warning: {e}")

def get_access_token(client_id, client_secret, refresh_token):
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Missing Google Drive OAuth credentials (client_id, client_secret, or refresh_token).")
    body = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data["access_token"]

def get_or_create_drive_folder(folder_name, parent_id, access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    q = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    
    url = f"https://www.googleapis.com/drive/v3/files?q={urllib.parse.quote(q)}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            files = r.json().get('files', [])
            if files:
                log(f"Found existing Drive folder '{folder_name}': {files[0]['id']}")
                return files[0]['id']
    except Exception as e:
        log(f"Folder query warning: {e}")

    # Create folder
    create_url = "https://www.googleapis.com/drive/v3/files"
    meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        meta["parents"] = [parent_id]
    
    try:
        cr = requests.post(create_url, headers=headers, json=meta, timeout=20)
        if cr.status_code in (200, 201):
            fid = cr.json()["id"]
            log(f"Created new Drive folder '{folder_name}': {fid}")
            # Try to make folder public viewable
            try:
                set_perm_url = f"https://www.googleapis.com/drive/v3/files/{fid}/permissions?supportsAllDrives=true"
                requests.post(set_perm_url, headers=headers, json={"role": "reader", "type": "anyone"}, timeout=15)
            except Exception:
                pass
            return fid
    except Exception as e:
        log(f"Folder creation warning: {e}")

    return parent_id

def download_file(url, target_path, access_token=None, cb_fn=None):
    # Check if Google Drive file ID
    gdrive_match = re.search(r'drive\.google\.com/(?:file/d/|open\?id=|uc\?id=)([a-zA-Z0-9_-]+)', url)
    if gdrive_match and access_token:
        file_id = gdrive_match.group(1)
        log(f"Downloading from Google Drive fileId: {file_id}")
        dl_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        headers = {"Authorization": f"Bearer {access_token}"}
        with requests.get(dl_url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            downloaded = 0
            t0 = time.time()
            last_cb = t0
            with open(target_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_cb > 4 and total_size > 0:
                            pct = min(99, int((downloaded / total_size) * 100))
                            speed_mb = (downloaded / max(1, (now - t0))) / (1024 * 1024)
                            log(f"Drive Download: {downloaded/(1024*1024):.1f} MB / {total_size/(1024*1024):.1f} MB ({pct}%) @ {speed_mb:.1f} MB/s")
                            if cb_fn:
                                cb_fn(pct, f"{speed_mb:.1f} MB/s")
                            last_cb = now
        return total_size or os.path.getsize(target_path)

    # Check Pixeldrain
    pd_match = re.search(r'pixeldrain\.com/u/([a-zA-Z0-9_-]+)', url)
    download_url = url
    if pd_match:
        download_url = f"https://pixeldrain.com/api/file/{pd_match.group(1)}?download"

    # Try aria2c first for multi-connection speed
    if shutil.which("aria2c"):
        log(f"Downloading via aria2c (16 connections): {download_url[:60]}...")
        cmd = [
            "aria2c", "-s", "16", "-x", "16", "-k", "2M",
            "--file-allocation=none", "--summary-interval=5",
            "--console-log-level=warn", "--check-certificate=false",
            "-o", os.path.basename(target_path),
            "-d", os.path.dirname(target_path),
            download_url
        ]
        ret = subprocess.run(cmd)
        if ret.returncode == 0 and os.path.exists(target_path) and os.path.getsize(target_path) > 1024:
            return os.path.getsize(target_path)
        log("aria2c failed or produced small file. Falling back to requests stream...")

    # Fallback to requests streaming
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(download_url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        t0 = time.time()
        last_cb = t0
        with open(target_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if now - last_cb > 4 and total_size > 0:
                        pct = min(99, int((downloaded / total_size) * 100))
                        speed_mb = (downloaded / max(1, (now - t0))) / (1024 * 1024)
                        log(f"Download: {downloaded/(1024*1024):.1f} MB / {total_size/(1024*1024):.1f} MB ({pct}%) @ {speed_mb:.1f} MB/s")
                        if cb_fn:
                            cb_fn(pct, f"{speed_mb:.1f} MB/s")
                        last_cb = now
    return total_size or os.path.getsize(target_path)

def probe_video(file_path):
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", file_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    info = json.loads(res.stdout)
    duration = float(info.get("format", {}).get("duration", 0))
    video_stream = next((s for s in info.get("streams", []) if s.get("codec_type") == "video"), None)
    audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    subtitle_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "subtitle"]
    
    height = int(video_stream.get("height", 0)) if video_stream else 0
    width = int(video_stream.get("width", 0)) if video_stream else 0
    return {
        "duration": duration,
        "width": width,
        "height": height,
        "audio_count": len(audio_streams),
        "subtitle_count": len(subtitle_streams)
    }

def smart_rename(original_title, target_res):
    """
    Intelligently replaces 1080p/2160p/4k/720p in the filename with target_res (e.g. 720p or 480p).
    If no resolution is found, cleanly appends [target_res] before the file extension.
    """
    clean_title = (original_title or '').strip()
    if not clean_title:
        clean_title = f"video_{target_res}.mkv"

    base, ext = os.path.splitext(clean_title)
    if not ext:
        ext = '.mkv'

    pattern = re.compile(r'(?i)\b(1080p|2160p|4k|720p|480p|360p)\b')
    if pattern.search(base):
        new_base = pattern.sub(target_res, base)
    else:
        new_base = f"{base} [{target_res}]"

    return f"{new_base}{ext}"

def compress_video(input_path, output_path, target_res, duration, cb_fn=None):
    res_map = {
        "720p": {"height": 720, "crf": "23", "preset": "medium"},
        "480p": {"height": 480, "crf": "24", "preset": "medium"},
        "360p": {"height": 360, "crf": "25", "preset": "faster"},
        "1080p": {"height": 1080, "crf": "22", "preset": "medium"},
    }
    spec = res_map.get(target_res, res_map["720p"])
    h = spec["height"]
    crf = spec["crf"]
    preset = spec["preset"]

    log(f"Compressing to {target_res} (height: {h}, crf: {crf}, preset: {preset})...")

    # Command preserves:
    # 1. Video scaled with high quality Lanczos / bicubic scaling
    # 2. All audio tracks untouched (-map 0:a? -c:a copy) preserving language names and sync
    # 3. All subtitle tracks untouched (-map 0:s? -c:s copy)
    cmd = [
        "ffmpeg", "-y", "-err_detect", "ignore_err",
        "-fflags", "+genpts+discardcorrupt",
        "-i", input_path,
        "-map", "0:v:0",
        "-vf", f"scale=-2:{h}",
        "-c:v", "libx264",
        "-preset", preset,
        "-crf", crf,
        "-map", "0:a?",
        "-c:a", "copy",
        "-map", "0:s?",
        "-c:s", "copy",
        "-max_muxing_queue_size", "2048",
        "-progress", "pipe:1",
        output_path
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last_cb = time.time()
    for line in proc.stdout:
        line = line.strip()
        if line.startswith("out_time_ms="):
            try:
                ms = int(line.split("=")[1])
                curr_sec = ms / 1000000.0
                if duration > 0:
                    pct = min(99, int((curr_sec / duration) * 100))
                    now = time.time()
                    if now - last_cb > 3:
                        log(f"   Compressing {target_res}: {pct}% ({curr_sec:.1f}s / {duration:.1f}s)")
                        if cb_fn:
                            cb_fn(pct, f"Encoding {target_res}")
                        last_cb = now
            except Exception:
                pass

    proc.wait()
    if proc.returncode != 0:
        # If copy audio failed (e.g. incompatible audio in container), fallback with audio re-encode to high quality AAC
        log(f"FFmpeg copy audio failed (code {proc.returncode}). Retrying with AAC audio encoding fallback...")
        cmd_fallback = [
            "ffmpeg", "-y", "-err_detect", "ignore_err",
            "-fflags", "+genpts+discardcorrupt",
            "-i", input_path,
            "-map", "0:v:0",
            "-vf", f"scale=-2:{h}",
            "-c:v", "libx264",
            "-preset", preset,
            "-crf", crf,
            "-map", "0:a?",
            "-c:a", "aac",
            "-b:a", "128k",
            "-map", "0:s?",
            "-c:s", "copy",
            "-max_muxing_queue_size", "2048",
            output_path
        ]
        ret_fallback = subprocess.run(cmd_fallback)
        if ret_fallback.returncode != 0:
            raise RuntimeError(f"FFmpeg compression to {target_res} failed with exit code {ret_fallback.returncode}")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise RuntimeError(f"Compressed file for {target_res} was not generated or is 0 bytes.")

    out_size = os.path.getsize(output_path)
    log(f"Compression to {target_res} complete: {out_size / (1024*1024):.2f} MB")
    return out_size

def upload_file_to_drive(file_path, file_name, folder_id, access_token, cb_fn=None):
    file_size = os.path.getsize(file_path)
    log(f"Uploading {file_name} ({file_size / (1024*1024):.2f} MB) to Google Drive...")

    meta = {
        "name": file_name,
        "parents": [folder_id] if folder_id else []
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": "application/octet-stream",
        "X-Upload-Content-Length": str(file_size)
    }

    init_res = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true",
        headers=headers,
        json=meta,
        timeout=45
    )
    init_res.raise_for_status()
    upload_url = init_res.headers.get("Location")
    if not upload_url:
        raise RuntimeError(f"Could not initialize Google Drive resumable upload session for {file_name}")

    chunk_size = 32 * 1024 * 1024
    with open(file_path, "rb") as f:
        offset = 0
        t0 = time.time()
        last_cb = t0
        while offset < file_size:
            f.seek(offset)
            chunk = f.read(chunk_size)
            chunk_len = len(chunk)
            up_headers = {
                "Content-Length": str(chunk_len),
                "Content-Range": f"bytes {offset}-{offset + chunk_len - 1}/{file_size}"
            }
            
            for retry in range(5):
                try:
                    r = requests.put(upload_url, headers=up_headers, data=chunk, timeout=180)
                    if r.status_code in (200, 201):
                        file_id = r.json().get("id")
                        log(f"Uploaded successfully to Drive! File ID: {file_id}")
                        # Make file public reader
                        try:
                            perm_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions?supportsAllDrives=true"
                            requests.post(perm_url, headers={"Authorization": f"Bearer {access_token}"}, json={"role": "reader", "type": "anyone"}, timeout=15)
                        except Exception:
                            pass
                        return file_id
                    elif r.status_code == 308:
                        offset += chunk_len
                        now = time.time()
                        if now - last_cb > 3 or offset >= file_size:
                            pct = min(99, int((offset / file_size) * 100))
                            speed_mb = (offset / max(1, (now - t0))) / (1024 * 1024)
                            log(f"   Drive Upload: {offset/(1024*1024):.1f} MB / {file_size/(1024*1024):.1f} MB ({pct}%) @ {speed_mb:.1f} MB/s")
                            if cb_fn:
                                cb_fn(pct, f"{speed_mb:.1f} MB/s")
                            last_cb = now
                        break
                    else:
                        log(f"   Drive upload chunk error (HTTP {r.status_code}): {r.text[:100]}. Retrying...")
                        time.sleep(3)
                except Exception as ex:
                    log(f"   Drive upload chunk network error: {ex}. Retrying {retry+1}/5...")
                    time.sleep(3 * (retry + 1))
            else:
                raise RuntimeError(f"Failed to upload chunk at offset {offset} to Google Drive after 5 retries.")

def main():
    payload_str = os.environ.get("PAYLOAD", "")
    if not payload_str:
        log("No PAYLOAD found in environment.")
        sys.exit(1)

    payload = json.loads(payload_str)
    task_id = payload.get("task_id") or payload.get("taskId")
    short_code = payload.get("short_code") or payload.get("shortCode") or task_id[:8]
    title = payload.get("title", "video.mkv")
    source_url = payload.get("source_url") or payload.get("sourceUrl")
    source_res = payload.get("source_resolution", "1080p")
    target_resolutions = payload.get("target_resolutions", ["720p"])
    folder_name = payload.get("folder_name", "MHBD_Compressed_Videos")
    callback_url = payload.get("callback_url")
    callback_secret = payload.get("callback_secret")
    drive_cfg = payload.get("drive_config", {})

    log(f"=== Starting Universal Video Compressor ===")
    log(f"Task ID: {task_id} ({short_code})")
    log(f"Title: {title}")
    log(f"Source Res: {source_res}")
    log(f"Targets: {target_resolutions}")
    log(f"Target Drive Folder: {folder_name}")

    def send_update(stage, progress, speed=None, original_size=0, compressed_size=0, output_files=None, err=None):
        callback(callback_url, callback_secret, {
            "task_id": task_id,
            "short_code": short_code,
            "status": "failed" if err else ("completed" if stage == "completed" else "processing"),
            "stage": stage,
            "progress": progress,
            "speed": speed,
            "original_size": original_size,
            "compressed_size": compressed_size,
            "output_files": output_files or [],
            "error_message": err
        })

    work_dir = os.path.join(os.getcwd(), f"work_{task_id[:8]}")
    os.makedirs(work_dir, exist_ok=True)
    input_path = os.path.join(work_dir, "input_source.mkv")

    try:
        # Step 1: Refresh Drive Token
        send_update("downloading", 2, "Connecting...")
        access_token = None
        if drive_cfg.get("client_id") and drive_cfg.get("refresh_token"):
            access_token = get_access_token(
                drive_cfg.get("client_id"),
                drive_cfg.get("client_secret"),
                drive_cfg.get("refresh_token")
            )
            log("Google Drive access token successfully acquired.")

        # Step 2: Download Source File
        def on_dl_progress(pct, spd):
            send_update("downloading", int(pct * 0.3), spd)

        original_size = download_file(source_url, input_path, access_token=access_token, cb_fn=on_dl_progress)
        log(f"Source downloaded: {original_size / (1024*1024):.2f} MB")
        send_update("downloading", 30, "Source Downloaded", original_size=original_size)

        # Step 3: Probe Video
        probe_info = probe_video(input_path)
        duration = max(1.0, probe_info["duration"])
        log(f"Probed video: duration={duration:.1f}s, size={probe_info['width']}x{probe_info['height']}, audio_tracks={probe_info['audio_count']}")

        # Step 4: Resolve Target Drive Folder
        target_folder_id = None
        if access_token:
            target_folder_id = get_or_create_drive_folder(folder_name, drive_cfg.get("folder_id"), access_token)

        # Step 5: Process Each Selected Resolution
        num_targets = len(target_resolutions)
        output_files = []
        total_compressed_size = 0

        for idx, t_res in enumerate(target_resolutions):
            out_filename = smart_rename(title, t_res)
            compressed_path = os.path.join(work_dir, f"out_{t_res}.mkv")

            # Progress calculation bracket for compression
            comp_start = 30 + int((idx / num_targets) * 50)
            comp_span = int(0.5 / num_targets * 50)

            def on_comp_progress(pct, spd):
                current_p = comp_start + int((pct / 100) * comp_span)
                send_update(f"compressing_{t_res}", current_p, spd, original_size=original_size, output_files=output_files)

            comp_size = compress_video(input_path, compressed_path, t_res, duration, cb_fn=on_comp_progress)
            total_compressed_size += comp_size

            # Upload to Google Drive
            up_start = comp_start + comp_span
            up_span = int(0.5 / num_targets * 20)

            def on_up_progress(pct, spd):
                current_p = up_start + int((pct / 100) * up_span)
                send_update(f"uploading_{t_res}", current_p, spd, original_size=original_size, compressed_size=total_compressed_size, output_files=output_files)

            drive_file_id = upload_file_to_drive(compressed_path, out_filename, target_folder_id, access_token, cb_fn=on_up_progress)
            drive_link = f"https://drive.google.com/file/d/{drive_file_id}/view?usp=sharing"

            output_files.append({
                "resolution": t_res,
                "fileName": out_filename,
                "fileSize": comp_size,
                "gdriveFileId": drive_file_id,
                "gdriveLink": drive_link,
                "status": "completed"
            })
            log(f"Resolution {t_res} successfully processed and uploaded: {drive_link}")

        # Step 6: All Completed Successfully!
        send_update(
            "completed", 100, "Done",
            original_size=original_size,
            compressed_size=total_compressed_size,
            output_files=output_files
        )
        log("🎉 All target resolutions processed and uploaded successfully!")

    except Exception as e:
        log(f"💥 Error in drive compressor: {e}")
        send_update("failed", 0, "Error", err=str(e))
        sys.exit(1)
    finally:
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
