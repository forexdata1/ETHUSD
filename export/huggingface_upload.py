"""Batch upload daily BarReplay BIN packs to a Hugging Face dataset.

Keeps index.txt in BRIDX1 format. Uses upload_folder so a backfill creates one
commit per run instead of one commit per day, avoiding HF commit-rate limits.
"""
from __future__ import annotations
import argparse, os, re, tempfile, time
from dataclasses import dataclass
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download

# Accept both true daily names and single-day range names created by pack_label:
#   XAUUSD_2013-01-01.BIN
#   XAUUSD_2013-01-01_2013-01-01.BIN
# The uploaded HF path is normalized to:
#   XAUUSD/XAUUSD_2013-01-01.BIN
DAILY_RE = re.compile(
    r"^(?P<symbol>.+?)_(?P<day>\d{4}-\d{2}-\d{2})(?:_\d{4}-\d{2}-\d{2})?\.BIN$",
    re.I,
)

@dataclass(frozen=True)
class Entry:
    symbol: str
    path: str
    day: str
    size: int
    source: Path | None = None

def retry(label, fn, attempts=5):
    delay=5; last=None
    for i in range(1, attempts+1):
        try: return fn()
        except Exception as e:
            last=e
            if i < attempts:
                print(f"{label} failed ({e}); retrying in {delay}s...")
                time.sleep(delay); delay=min(delay*2, 60)
    raise RuntimeError(f"{label} failed after {attempts} attempts: {last}")

def read_index(repo_id, repo_type, token):
    try:
        with tempfile.TemporaryDirectory() as td:
            p=hf_hub_download(repo_id=repo_id, repo_type=repo_type, filename="index.txt", token=token, local_dir=td)
            text=Path(p).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {}
    out={}
    for line in text.splitlines():
        if not line.startswith("FILE|"): continue
        parts=line.split("|")
        if len(parts) < 4: continue
        try:
            rel, day, size = parts[1], parts[2], int(parts[3])
            # Keep only normalized catalog rows. Bad old rows like
            # XAUUSD_2013-01-01/... are intentionally dropped from index.txt.
            if "/" not in rel:
                continue
            folder, name = rel.split("/", 1)
            m = DAILY_RE.match(name)
            if not m:
                continue
            sym = m.group("symbol").upper()
            normalized = f"{sym}/{sym}_{m.group('day')}.BIN"
            out[normalized] = Entry(sym, normalized, day, size)
        except ValueError:
            pass
    return out

def local_entries(folder: Path):
    out={}
    for p in folder.rglob("*.BIN"):
        m=DAILY_RE.match(p.name)
        if not m: continue
        sym=m.group("symbol").upper(); day=m.group("day")
        rel=f"{sym}/{sym}_{day}.BIN"
        out[rel]=Entry(sym, rel, day, p.stat().st_size, p)
    return out

def index_text(entries):
    by={}
    for e in entries.values(): by.setdefault(e.symbol, []).append(e)
    lines=["BRIDX1"]
    for sym in sorted(by):
        lines.append(f"SYM|{sym}|{sym}|0|5")
        for e in sorted(by[sym], key=lambda x:x.day):
            lines.append(f"FILE|{e.path}|{e.day}|{e.size}")
    return "\n".join(lines)+"\n"

def upload_batch(repo_id, source_dir: Path, token, repo_type="dataset"):
    if not token: raise SystemExit("error: HF_TOKEN is required")
    api=HfApi(token=token)
    api.create_repo(repo_id=repo_id, repo_type=repo_type, exist_ok=True, private=False)
    existing=read_index(repo_id, repo_type, token)
    local=local_entries(source_dir)
    if not local:
        print(f"No daily .BIN files found in {source_dir}; skipping HF upload."); return
    changed={k:v for k,v in local.items() if k not in existing or existing[k].size != v.size}
    merged=dict(existing); merged.update(local)
    with tempfile.TemporaryDirectory() as td:
        stage=Path(td)/"stage"; stage.mkdir()
        for rel, entry in changed.items():
            if entry.source is None:
                continue
            dst=stage/rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(entry.source.read_bytes())
        (stage/"index.txt").write_text(index_text(merged), encoding="utf-8")
        print(f"HF upload: {len(changed)} changed BIN file(s) + index.txt -> {repo_id}")
        retry("upload_folder", lambda: api.upload_folder(repo_id=repo_id, repo_type=repo_type, folder_path=stage, path_in_repo="", token=token, commit_message=f"Upload daily tick data ({len(changed)} files)"))

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("source_dir"); ap.add_argument("--repo-id", default=os.getenv("HF_DATASET","Esmaeil9ss/Tickdata")); ap.add_argument("--repo-type", default="dataset"); ap.add_argument("--token", default=os.getenv("HF_TOKEN")); a=ap.parse_args()
    upload_batch(a.repo_id, Path(a.source_dir), a.token, a.repo_type); return 0
if __name__ == "__main__": raise SystemExit(main())
