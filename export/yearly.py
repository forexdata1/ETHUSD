"""Period packaging: merge the hour .bin files of a downloaded range into a
single MT5-ready BIN, plus a zip helper.

bin_v1 hour blocks are self-delimiting (uint32 tick_count + columns), so
concatenating hour files byte-for-byte in chronological order produces a valid
combined file — the same layout MT5 imports (see storage.tick_format).

Pack naming follows the period covered:
    full calendar year   -> <SYMBOL>_<YEAR>.BIN         (EURUSD_2025.BIN)
    full calendar month  -> <SYMBOL>_<YEAR>-<MM>.BIN    (EURUSD_2025-01.BIN)
    single day           -> <SYMBOL>_<YYYY-MM-DD>.BIN   (EURUSD_2025-01-15.BIN)
    any other range      -> <SYMBOL>_<START>_<END>.BIN

Zip naming drops the .BIN suffix: EURUSD_2025.BIN -> EURUSD_2025.zip
"""
from __future__ import annotations

import calendar
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from core.models.instrument import Instrument
from storage.tick_format import read_hour_tick_count
from storage.tick_storage import TickStorage

COPY_BUFFER = 16 * 1024 * 1024


def yearly_output_dir(data_dir: Path) -> Path:
    """Directory that receives merged packs (all periods, not only years)."""
    return data_dir / "yearly"


def pack_label(start: date, end: date) -> str:
    """Period label for a range: year, year-month, day, or explicit range."""
    if start == date(start.year, 1, 1) and end == date(start.year, 12, 31):
        return f"{start.year}"
    if (
        start.day == 1
        and (end.year, end.month) == (start.year, start.month)
        and end.day == calendar.monthrange(start.year, start.month)[1]
    ):
        return f"{start.year}-{start.month:02d}"
    if start == end:
        return start.isoformat()
    return f"{start.isoformat()}_{end.isoformat()}"


def pack_name(symbol: str, start: date, end: date) -> str:
    return f"{symbol}_{pack_label(start, end)}.BIN"


def _day_bounds_utc(start: date, end: date) -> tuple[datetime, datetime]:
    return (
        datetime(start.year, start.month, start.day, 0, tzinfo=timezone.utc),
        datetime(end.year, end.month, end.day, 23, tzinfo=timezone.utc),
    )


def merge_range(
    storage: TickStorage,
    instrument: Instrument,
    start: date,
    end: date,
    out_dir: Path,
) -> tuple[Path, int] | None:
    """Merge all stored hour files in [start, end] into one pack BIN.

    Hour files are appended in chronological order without modification, so the
    result stays importable by MT5. Written atomically (temp file + rename).
    Returns (output_path, total_ticks), or None when no data exists in the range.
    """
    start_dt, end_dt = _day_bounds_utc(start, end)
    hours = storage.list_stored_hours(instrument, start_dt, end_dt)
    if not hours:
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / pack_name(instrument.symbol, start, end)
    tmp_path = out_path.with_name(out_path.name + ".tmp")

    total_ticks = 0
    with open(tmp_path, "wb", buffering=COPY_BUFFER) as out:
        for hour in hours:
            src = storage.hour_path(instrument, hour)
            if not src.is_file():
                continue
            total_ticks += read_hour_tick_count(src)
            with open(src, "rb") as fin:
                while True:
                    chunk = fin.read(COPY_BUFFER)
                    if not chunk:
                        break
                    out.write(chunk)
    tmp_path.replace(out_path)
    return out_path, total_ticks


def merge_year(
    storage: TickStorage,
    instrument: Instrument,
    year: int,
    out_dir: Path,
) -> tuple[Path, int] | None:
    """Merge one full calendar year into <SYMBOL>_<YEAR>.BIN."""
    return merge_range(
        storage, instrument, date(year, 1, 1), date(year, 12, 31), out_dir
    )


def zip_yearly_bin(bin_path: Path) -> Path:
    """Zip a pack BIN -> <SYMBOL>_<PERIOD>.zip (atomically, same directory).

    The .BIN suffix is dropped: EURUSD_2025.BIN -> EURUSD_2025.zip.
    """
    zip_path = bin_path.with_suffix(".zip")
    tmp_path = zip_path.with_name(zip_path.name + ".tmp")
    with zipfile.ZipFile(
        tmp_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        allowZip64=True,
    ) as zf:
        zf.write(bin_path, arcname=bin_path.name)
    tmp_path.replace(zip_path)
    return zip_path
