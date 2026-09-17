"""Build MT5-ready daily BIN packs from downloaded hourly files.

Even when start/end cover several days, this script writes one output file per
calendar day so Hugging Face receives:
    XAUUSD/XAUUSD_YYYY-MM-DD.BIN
not one combined multi-day BIN.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# When this file is executed as `python export/daily_pack.py`, Python puts
# `export/` on sys.path, not the repository root. Add the repo root so imports
# like `config.settings` work in GitHub Actions.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import Settings
from core.services.instrument_search import InstrumentCatalog
from export.yearly import merge_range, yearly_output_dir
from storage.tick_storage import TickStorage


def parse_date(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("start", type=parse_date)
    ap.add_argument("end", type=parse_date)
    args = ap.parse_args()

    settings = Settings()
    settings.ensure_directories()
    instrument = InstrumentCatalog(settings.instruments_file).get(args.symbol)
    storage = TickStorage(settings.data_dir)
    out_dir = yearly_output_dir(settings.data_dir)

    if args.end < args.start:
        raise SystemExit("error: end date is before start date")

    made = 0
    ticks_total = 0
    day = args.start
    while day <= args.end:
        result = merge_range(storage, instrument, day, day, out_dir)
        if result is None:
            print(f"No stored ticks for {instrument.symbol} {day}; skipping daily pack.")
        else:
            path, ticks = result
            made += 1
            ticks_total += ticks
            print(f"Packed {path} ({ticks:,} ticks, {path.stat().st_size:,} bytes)")
        day += timedelta(days=1)

    print(f"Daily pack summary: {made} file(s), {ticks_total:,} ticks total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
