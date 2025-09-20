#!/usr/bin/env python3
import os, sys, json, time, shutil, socket, pathlib
from datetime import datetime, timezone
import yaml

# Optional deps by backend:
#   pip install boto3         (for S3)
#   pip install paramiko      (for SFTP)
#   pip install requests      (for quick connectivity check, otherwise use socket)
try:
    import requests
except Exception:
    requests = None

DATA_GLOB_DEPTH = 4  # /YYYY/MM/DD/HHMMSSZ/

def internet_up(timeout=2.0) -> bool:
    """
    Returns True if we appear to have internet.
    Strategy: try a very fast HTTP request; fall back to DNS socket if requests missing.
    """
    try:
        if requests:
            r = requests.get("https://www.google.com/generate_204", timeout=timeout)
            return r.status_code == 204
        else:
            # DNS socket fallback (1.1.1.1:53)
            with socket.create_connection(("1.1.1.1", 53), timeout=timeout):
                return True
    except Exception:
        return False

def find_pending_batches(data_root: str):
    """
    Yield absolute paths to batch dirs that look like .../YYYY/MM/DD/HHMMSSZ/
    and do NOT have a '.uploaded' file.
    """
    root = pathlib.Path(data_root)
    if not root.exists():
        return
    for year_dir in root.glob("[0-9][0-9][0-9][0-9]"):
        for month_dir in year_dir.glob("[0-1][0-9]"):
            for day_dir in month_dir.glob("[0-3][0-9]"):
                for batch_dir in day_dir.glob("*Z"):
                    if batch_dir.is_dir():
                        if not (batch_dir / ".uploaded").exists() and not (batch_dir / ".uploading").exists():
                            yield batch_dir

def mark_uploading(batch_dir: pathlib.Path):
    (batch_dir / ".uploading").write_text(datetime.now(timezone.utc).isoformat())

def mark_uploaded(batch_dir: pathlib.Path):
    # Make idempotent: remove uploading marker and add uploaded marker
    try:
        (batch_dir / ".uploading").unlink(missing_ok=True)
    except TypeError:
        # Python <3.8 compatibility
        try:
            (batch_dir / ".uploading").unlink()
        except FileNotFoundError:
            pass
    (batch_dir / ".uploaded").write_text(datetime.now(timezone.utc).isoformat())

# ----------------- Backends -----------------

def upload_batch_local(batch_dir: pathlib.Path, dest_dir: str):
    dest = pathlib.Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    # Copy (or move) the whole directory tree—copy keeps a local archive; move is also fine.
    dst = dest / batch_dir.name
    if dst.exists():
        # avoid clobber: add suffix
        dst = dest / f"{batch_dir.name}-{int(time.time())}"
    shutil.copytree(batch_dir, dst)
    return True

def upload_batch_s3(batch_dir: pathlib.Path, cfg: dict):
    import boto3
    s3 = boto3.client("s3", region_name=cfg.get("region"))
    bucket = cfg["bucket"]
    prefix = cfg.get("prefix", "")
    # Use batch folder name (e.g., 153000Z) and its YYYY/MM/DD parents in the key
    # Build relative path from data_root
    files = [p for p in batch_dir.iterdir() if p.is_file()]
    # Upload each file
    # Key structure: prefix/YYYY/MM/DD/HHMMSSZ/filename
    parts = list(batch_dir.parts)
    # find last 4 (YYYY/MM/DD/HHMMSSZ)
    yyyy, mm, dd, stamp = parts[-4], parts[-3], parts[-2], parts[-1]
    for f in files:
        key = f"{prefix}{yyyy}/{mm}/{dd}/{stamp}/{f.name}"
        s3.upload_file(str(f), bucket, key)
    return True

def upload_batch_sftp(batch_dir: pathlib.Path, cfg: dict):
    import paramiko
    host = cfg["host"]; port = cfg.get("port", 22)
    username = cfg["username"]
    remote_dir = cfg["remote_dir"]
    pkey_path = cfg.get("private_key")
    password = cfg.get("password")  # discourage, but allow

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if pkey_path:
        key = paramiko.Ed25519Key.from_private_key_file(pkey_path) if pkey_path.endswith("ed25519") \
              else paramiko.RSAKey.from_private_key_file(pkey_path)
        ssh.connect(host, port=port, username=username, pkey=key, timeout=10)
    else:
        ssh.connect(host, port=port, username=username, password=password, timeout=10)
    sftp = ssh.open_sftp()

    # Create remote subdir: remote_dir/YYYY/MM/DD/HHMMSSZ/
    yyyy, mm, dd, stamp = batch_dir.parts[-4], batch_dir.parts[-3], batch_dir.parts[-2], batch_dir.parts[-1]
    sub = f"{remote_dir}/{yyyy}/{mm}/{dd}/{stamp}"
    # ensure path exists
    def sftp_mkdir_p(path):
        parts = path.strip("/").split("/")
        cur = ""
        for p in parts:
            cur = f"{cur}/{p}" if cur else f"/{p}"
            try:
                sftp.stat(cur)
            except IOError:
                sftp.mkdir(cur)
    sftp_mkdir_p(sub)

    for f in batch_dir.iterdir():
        if f.is_file():
            sftp.put(str(f), f"{sub}/{f.name}")

    sftp.close()
    ssh.close()
    return True

# ----------------- Main runner -----------------

def main():
    cfg_path = "/etc/companion/config.yaml" if os.path.exists("/etc/companion/config.yaml") else "config.yaml"
    with open(cfg_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    data_root = cfg["data_root"]
    backend = cfg["uploader"]["backend"]

    if not internet_up():
        print("[uploader] No internet — exiting.")
        return 0

    pending = list(find_pending_batches(data_root))
    if not pending:
        print("[uploader] Nothing to upload.")
        return 0

    for batch_dir in sorted(pending):
        try:
            print(f"[uploader] Processing {batch_dir}")
            mark_uploading(batch_dir)

            if backend == "local":
                ok = upload_batch_local(batch_dir, cfg["uploader"]["local"]["dest_dir"])
            elif backend == "s3":
                ok = upload_batch_s3(batch_dir, cfg["uploader"]["s3"])
            elif backend == "sftp":
                ok = upload_batch_sftp(batch_dir, cfg["uploader"]["sftp"])
            else:
                print(f"[uploader] Unknown backend: {backend}")
                ok = False

            if ok:
                mark_uploaded(batch_dir)
                print(f"[uploader] Uploaded OK: {batch_dir}")
            else:
                # leave .uploading to help you diagnose; you can remove it manually if needed
                print(f"[uploader] Upload FAILED: {batch_dir}")

        except Exception as e:
            print(f"[uploader] Error on {batch_dir}: {e}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
