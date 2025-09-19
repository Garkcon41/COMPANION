#!/usr/bin/env python3
import os, json, time, pathlib, yaml
from datetime import datetime, timezone

def iso_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def atomic_write(path, data_bytes):
    tmp = f"{path}.tmp"
    with open(tmp, "wb") as f: f.write(data_bytes)
    os.replace(tmp, path)

with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

root = pathlib.Path(cfg["data_root"]); root.mkdir(parents=True, exist_ok=True)

now = datetime.now(timezone.utc).replace(microsecond=0)
yyyy, mm, dd = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
stamp = now.strftime("%H%M%SZ")
batch = root / yyyy / mm / dd / stamp
batch.mkdir(parents=True, exist_ok=True)

cam_status = []
for name, src in cfg["cameras"]["mock_files"].items():
    try:
        data = open(src, "rb").read()
        atomic_write(batch / f"{name}.jpg", data)
        cam_status.append({"name": name, "filename": f"{name}.jpg", "ok": True})
    except Exception as e:
        cam_status.append({"name": name, "filename": f"{name}.jpg", "ok": False, "error": str(e)})

fix = cfg["gnss"]["mock_fix"].copy()
time.sleep(fix.get("delay_ms", 0)/1000.0)
fix.update({"stale": False, "source": "mock", "fix_timestamp_utc": iso_utc()})

meta = {
    "batch_id": now.isoformat(),
    "timestamp_utc": now.isoformat(),
    "gps": fix,
    "cameras": cam_status,
    "notes": []
}
atomic_write(batch / "meta.json", json.dumps(meta, indent=2).encode("utf-8"))
print(f"Saved batch: {batch}")
