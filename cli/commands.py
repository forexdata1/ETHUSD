"""Command line interface.

    python main.py search <text>
    python main.py download <SYMBOL> <START> <END> [--workers N] [--force] [--profile]
                            [--yearly] [--upload-release] [--repo owner/name]
    python main.py gaps     <SYMBOL> <START> <END> [--repair]
    python main.py gaps     <SYMBOL> --all [--repair]
    python main.py status   <SYMBOL>
    python main.py migrate  [--dry-run] [--keep-parquet]
    python main.py web      [--host HOST] [--port PORT]
"""
from __future__ import annotations

import argparse
import sys
import threading
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

from config.settings import Settings
from core.models.instrument import Instrument
from core.models.task import TaskStatus
from core.services.download_engine import DownloadEngine
from core.services.gap_scanner import GapScanner
from core.services.instrument_search import InstrumentCatalog, UnknownInstrumentError
from core.services.planner import Planner
from core.services.profile_format import format_profile_line
from export.yearly import merge_range, pack_label, yearly_output_dir, zip_yearly_bin
from storage.metadata_db import MetadataDB, _hour_key
from storage.tick_storage import TickStorage


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dukascopy-downloader",
        description="Download Dukascopy tick data into MT5-ready binary files and import to MT5.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_search = sub.add_parser("search", help="search the instrument catalog")
    p_search.add_argument("query")

    def add_range_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("symbol")
        p.add_argument("start", type=_parse_date, help="start date YYYY-MM-DD (inclusive)")
        p.add_argument("end", type=_parse_date, help="end date YYYY-MM-DD (inclusive)")

    p_download = sub.add_parser("download", help="download ticks into binary tick storage")
    add_range_args(p_download)
    p_download.add_argument(
        "--workers", type=int, default=None,
        help="max concurrent downloads (adaptive ceiling, default from settings)",
    )
    p_download.add_argument("--force", action="store_true",
                            help="re-process hours even if marked completed/empty")
    p_download.add_argument(
        "--profile", action="store_true",
        help="print per-hour fetch/decode/write timings (ms)",
    )
    p_download.add_argument(
        "--yearly", action="store_true",
        help="process a multi-year range one calendar year at a time "
             "(one merged pack per year)",
    )
    p_download.add_argument(
        "--upload-release", action="store_true",
        help="after downloading, merge the downloaded range into one BIN pack, "
             "zip it and upload it to a GitHub release (tag <SYMBOL>-<period>); "
             "needs GITHUB_TOKEN plus --repo or GITHUB_REPOSITORY",
    )
    p_download.add_argument(
        "--repo", default=None,
        help="GitHub repository as owner/name for --upload-release "
             "(default: GITHUB_REPOSITORY env var)",
    )
    p_download.add_argument(
        "--min-lag-hours", type=int, default=None,
        help="override the recent-data lag in hours (default 2 from settings). "
             "Scheduled jobs pass 0: their end date is already in the past, and "
             "the default lag would drop each period's final hour",
    )
    p_download.add_argument(
        "--notify-telegram", action="store_true",
        help="after each pack, send it to a Telegram channel via a bot "
             "(TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID env vars). Packs over the "
             "50 MB bot upload limit fall back to a message with the release link",
    )

    p_gaps = sub.add_parser("gaps", help="report (and optionally repair) missing hours")
    p_gaps.add_argument("symbol")
    p_gaps.add_argument(
        "start",
        nargs="?",
        type=_parse_date,
        help="start date YYYY-MM-DD (required unless --all)",
    )
    p_gaps.add_argument(
        "end",
        nargs="?",
        type=_parse_date,
        help="end date YYYY-MM-DD (required unless --all)",
    )
    p_gaps.add_argument(
        "--all",
        action="store_true",
        help="scan the full recorded range for this symbol (no start/end dates)",
    )
    p_gaps.add_argument("--repair", action="store_true", help="download missing/failed hours")
    p_gaps.add_argument(
        "--workers", type=int, default=None,
        help="max concurrent downloads (same as download command)",
    )
    p_gaps.add_argument(
        "--refetch-empty",
        action="store_true",
        help="with --repair, also re-request hours already marked empty",
    )

    p_status = sub.add_parser("status", help="show stored data summary for an instrument")
    p_status.add_argument("symbol")

    p_migrate = sub.add_parser(
        "migrate",
        help="convert legacy .parquet files to .bin (one-time after upgrading)",
    )
    p_migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="list files that would be converted",
    )
    p_migrate.add_argument(
        "--keep-parquet",
        action="store_true",
        help="keep .parquet files after conversion",
    )

    p_web = sub.add_parser("web", help="start the web UI")
    p_web.add_argument("--host", default="127.0.0.1")
    p_web.add_argument("--port", type=int, default=8080)

    return parser


def _validate_range(start: date, end: date) -> None:
    if end < start:
        raise SystemExit("error: end date is before start date")


def cmd_search(catalog: InstrumentCatalog, args) -> int:
    results = catalog.search(args.query)
    if not results:
        print(f"No instruments match '{args.query}'.")
        return 1
    print(f"{len(results)} instrument(s) match '{args.query}':\n")
    print(f"{'SYMBOL':<14} {'NAME':<16} {'DECIMALS':>8}  {'SINCE':<12} DESCRIPTION")
    for inst in results[:50]:
        since = inst.earliest_tick_utc.date().isoformat() if inst.earliest_tick_utc else "?"
        print(f"{inst.symbol:<14} {inst.name:<16} {inst.price_decimals:>8}  "
              f"{since:<12} {inst.description}")
    if len(results) > 50:
        print(f"... and {len(results) - 50} more (refine your query)")
    return 0


def _error_bucket(error: str | None) -> str:
    """Group similar errors so the failure summary stays readable."""
    if not error:
        return "unknown error"
    for prefix in ("network error", "verification failed", "invalid JSON"):
        if error.startswith(prefix):
            return prefix + " (…)"
    if error.startswith("HTTP "):
        return error.split(":", 1)[0]
    return error[:80]


def _print_failure_breakdown(failed_tasks) -> None:
    """Show what the failed hours actually failed with (counts by cause)."""
    from collections import Counter

    counts = Counter(_error_bucket(task.error) for task in failed_tasks)
    if not counts:
        return
    print("\nFailure breakdown (hours × cause):")
    for cause, count in counts.most_common(10):
        print(f"  {count:>5} × {cause}")


def _run_download(
    settings: Settings,
    instrument: Instrument,
    start: date,
    end: date,
    args,
) -> int:
    """Plan and download one date range (single-shot behaviour)."""
    _validate_range(start, end)
    local_settings = settings.for_job(args.workers)
    if getattr(args, "min_lag_hours", None) is not None:
        local_settings = replace(
            local_settings, min_data_lag_hours=args.min_lag_hours
        )

    db = MetadataDB(settings.db_path)
    storage = TickStorage(settings.data_dir)
    planner = Planner(local_settings, db)
    engine = DownloadEngine(local_settings, storage, db)

    plan = planner.plan(instrument, start, end, force=args.force)
    if plan.effective_start is None:
        print("Nothing to do: no downloadable hours in this range "
              "(check the instrument's data start date and the recent-data lag).")
        return 0
    print(f"Range      : {plan.effective_start:%Y-%m-%d %H:%M} -> "
          f"{plan.effective_end:%Y-%m-%d %H:%M} UTC ({plan.total_hours} hours)")
    print(f"Plan       : {len(plan.tasks)} to download, {plan.already_done} already done\n")

    if not plan.tasks:
        print("All hours already downloaded. Nothing to do.")
        return 0

    if args.profile:
        print("Profile (ms): fetch = HTTP, decode = JSON+verify, write = binary+ledger\n")

    log_lock = threading.Lock()

    def on_task_done(task) -> None:
        if not args.profile:
            return
        with log_lock:
            print(format_profile_line(task), flush=True)

    stats = engine.run(
        plan.tasks,
        refetch=args.force,
        profile=args.profile,
        on_task_done=on_task_done if args.profile else None,
    )
    print(f"\nDone: {stats.completed} hours with data, {stats.empty} empty, "
          f"{stats.failed} failed, {stats.ticks:,} ticks total.")
    if stats.failed:
        _print_failure_breakdown(stats.failed_tasks)
        print("Some hours kept failing; run later:\n"
              f"  python main.py gaps {instrument.symbol} {start} {end} --repair")
        return 1
    return 0


def _release_credentials(args):
    """Resolve and validate GitHub release credentials before downloading."""
    from export.github_release import (
        ReleaseUploadError,
        resolve_repository,
        resolve_token,
    )

    try:
        return resolve_token(), resolve_repository(args.repo)
    except ReleaseUploadError as exc:
        raise SystemExit(f"error: {exc}")


def _telegram_credentials(args):
    """Resolve and validate Telegram bot credentials before downloading."""
    from export.telegram_notify import (
        TelegramNotifyError,
        resolve_bot_token,
        resolve_chat_id,
    )

    try:
        return resolve_bot_token(), resolve_chat_id()
    except TelegramNotifyError as exc:
        raise SystemExit(f"error: {exc}")


def _process_pack(
    args, token, repo, tg_token, tg_chat, instrument, start, end, bin_path
) -> bool:
    """Zip one merged pack, then upload it to a GitHub release and/or send it
    to a Telegram channel (whichever was requested)."""
    label = pack_label(start, end)
    zip_path = zip_yearly_bin(bin_path)
    zip_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"[{label}] zipped -> {zip_path.name} ({zip_mb:,.1f} MB)")

    ok = True
    download_url = None
    if args.upload_release:
        from export.github_release import (
            ReleaseUploadError,
            get_or_create_release,
            upload_asset,
        )

        tag = f"{instrument.symbol}-{label}"
        try:
            release = get_or_create_release(
                repo, tag, token, name=f"{instrument.symbol} {label} tick data"
            )
            download_url = upload_asset(repo, release, zip_path, token)
            print(f"[{label}] uploaded -> {download_url or 'release ' + tag}")
        except ReleaseUploadError as exc:
            ok = False
            print(f"[{label}] error: release upload failed: {exc}", file=sys.stderr)

    if args.notify_telegram:
        from export.telegram_notify import TelegramNotifyError, notify_pack

        caption = f"📊 {instrument.symbol} — {label}\n📦 {zip_path.name} ({zip_mb:,.1f} MB)"
        try:
            mode = notify_pack(tg_token, tg_chat, zip_path, caption, download_url)
            print(f"[{label}] telegram -> {mode}")
        except TelegramNotifyError as exc:
            ok = False
            print(f"[{label}] error: telegram notify failed: {exc}", file=sys.stderr)
    print()
    return ok


def _run_download_yearly(settings: Settings, instrument: Instrument, args) -> int:
    """Download a range one calendar year at a time, merging each finished
    chunk into a pack under data/yearly/ and optionally publishing it."""
    token = repo = tg_token = tg_chat = None
    if args.upload_release:
        token, repo = _release_credentials(args)
    if args.notify_telegram:
        tg_token, tg_chat = _telegram_credentials(args)

    storage = TickStorage(settings.data_dir)
    out_dir = yearly_output_dir(settings.data_dir)
    years = list(range(args.start.year, args.end.year + 1))
    print(f"Yearly mode: {len(years)} year(s), {years[0]} -> {years[-1]}\n")

    merged: list[Path] = []
    failed = False
    for year in years:
        year_start = max(args.start, date(year, 1, 1))
        year_end = min(args.end, date(year, 12, 31))
        print(f"===== {instrument.symbol} {year}: {year_start} -> {year_end} =====")
        if _run_download(settings, instrument, year_start, year_end, args):
            failed = True

        result = merge_range(storage, instrument, year_start, year_end, out_dir)
        if result is None:
            print(f"[{year}] no stored hours — skipping merge.\n")
            continue
        bin_path, tick_count = result
        size_mb = bin_path.stat().st_size / (1024 * 1024)
        print(f"[{pack_label(year_start, year_end)}] merged -> {bin_path.name} "
              f"({tick_count:,} ticks, {size_mb:,.1f} MB)")
        merged.append(bin_path)

        if args.upload_release or args.notify_telegram:
            if not _process_pack(
                args, token, repo, tg_token, tg_chat,
                instrument, year_start, year_end, bin_path,
            ):
                failed = True

    print(f"\nYearly summary ({len(merged)} pack(s) in {out_dir}):")
    for path in merged:
        print(f"  {path.name}")
    return 1 if failed else 0


def cmd_download(settings: Settings, catalog: InstrumentCatalog, args) -> int:
    instrument = catalog.get(args.symbol)
    _validate_range(args.start, args.end)
    print(f"Instrument : {instrument.name} ({instrument.symbol}), "
          f"{instrument.price_decimals} decimals")
    if args.yearly:
        return _run_download_yearly(settings, instrument, args)

    token = repo = tg_token = tg_chat = None
    if args.upload_release:
        token, repo = _release_credentials(args)
    elif args.repo:
        print("warning: --repo is ignored without --upload-release.",
              file=sys.stderr)
    if args.notify_telegram:
        tg_token, tg_chat = _telegram_credentials(args)

    rc = _run_download(settings, instrument, args.start, args.end, args)
    if not (args.upload_release or args.notify_telegram):
        return rc

    # Daily / monthly / arbitrary ranges: merge the downloaded range into one
    # pack, then upload to a GitHub release and/or notify Telegram.
    storage = TickStorage(settings.data_dir)
    result = merge_range(
        storage, instrument, args.start, args.end,
        yearly_output_dir(settings.data_dir),
    )
    if result is None:
        print("No stored hours in this range — nothing to pack.")
        return rc
    bin_path, tick_count = result
    size_mb = bin_path.stat().st_size / (1024 * 1024)
    print(f"Merged -> {bin_path.name} ({tick_count:,} ticks, {size_mb:,.1f} MB)")
    if rc:
        print("warning: some hours failed — the pack covers completed hours only.")
    if not _process_pack(args, token, repo, tg_token, tg_chat,
                         instrument, args.start, args.end, bin_path):
        return 1
    return rc


def cmd_gaps(settings: Settings, catalog: InstrumentCatalog, args) -> int:
    instrument = catalog.get(args.symbol)
    if args.all and (args.start or args.end):
        raise SystemExit("error: --all cannot be combined with start/end dates")
    if not args.all and (args.start is None or args.end is None):
        raise SystemExit("error: start and end dates are required (or use --all)")

    db = MetadataDB(settings.db_path)
    scanner = GapScanner(settings, db)

    if args.all:
        report = scanner.scan_all(instrument)
        if report is None:
            print(f"No data recorded for {instrument.symbol} yet.")
            return 0
        span = db.recorded_span(instrument.id)
        range_label = (
            f"{span[0]:%Y-%m-%d %H:%M} -> {span[1]:%Y-%m-%d %H:%M} UTC (all recorded)"
        )
    else:
        _validate_range(args.start, args.end)
        report = scanner.scan(instrument, args.start, args.end)
        range_label = f"{args.start} -> {args.end}"

    print(f"{instrument.symbol} {range_label}: "
          f"{report.total_hours} hours total, {report.completed} with data, "
          f"{report.empty} empty, {len(report.failed_hours)} failed, "
          f"{len(report.missing_hours)} never attempted")

    repair_hours = report.repair_hours(refetch_empty=args.refetch_empty)
    if not repair_hours:
        print("Dataset is complete.")
        return 0

    if not args.repair:
        if report.gap_hours:
            print(f"Gap hours ({len(report.gap_hours)}):")
            for hour in report.gap_hours:
                print(f"  {hour:%Y-%m-%d %H:00}")
            print("Run with --repair to download them.")
        if report.empty_hours:
            print(
                f"Also {len(report.empty_hours)} hour(s) marked empty "
                f"(use --repair --refetch-empty to re-request them)."
            )
        return 1

    local_settings = settings.for_job(args.workers)
    storage = TickStorage(settings.data_dir)
    engine = DownloadEngine(local_settings, storage, db)
    tasks = scanner.build_repair_tasks(instrument, report, refetch_empty=args.refetch_empty)
    parts = []
    if report.gap_hours:
        parts.append(f"{len(report.gap_hours)} gap")
    if args.refetch_empty and report.empty_hours:
        parts.append(f"{len(report.empty_hours)} empty refetch")

    reasons: dict[str, str] = {}
    for hour in report.gap_hours:
        reasons[_hour_key(hour)] = "gap"
    for hour in report.empty_hours:
        reasons[_hour_key(hour)] = "empty refetch"

    print(f"Repairing {len(tasks)} hour(s) ({', '.join(parts)}):")
    for task in sorted(tasks, key=lambda t: t.hour):
        reason = reasons.get(_hour_key(task.hour), "repair")
        print(f"  {task.hour:%Y-%m-%d %H:00} UTC  [{reason}]")
    print()

    log_lock = threading.Lock()

    def on_task_done(task) -> None:
        reason = reasons.get(_hour_key(task.hour), "repair")
        with log_lock:
            if task.status is TaskStatus.COMPLETED:
                print(f"  {task.hour:%Y-%m-%d %H:00} UTC  [{reason}] -> {task.tick_count:,} ticks")
            elif task.status is TaskStatus.EMPTY:
                print(f"  {task.hour:%Y-%m-%d %H:00} UTC  [{reason}] -> still empty")
            else:
                print(f"  {task.hour:%Y-%m-%d %H:00} UTC  [{reason}] -> failed: {task.error}")

    stats = engine.run(tasks, on_task_done=on_task_done, refetch=True)
    print(f"\nDone: {stats.completed} hours with data, {stats.empty} empty, "
          f"{stats.failed} failed, {stats.ticks:,} ticks total.")
    return 1 if stats.failed else 0


def _port_available(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _resolve_web_port(host: str, port: int) -> int:
    if _port_available(host, port):
        return port
    for candidate in range(port + 1, port + 20):
        if _port_available(host, candidate):
            print(f"Port {port} is already in use, using {candidate} instead.")
            return candidate
    raise SystemExit(
        f"error: no free port found near {port}. "
        f"Stop the other process or run: python main.py web --port 9000"
    )


def cmd_migrate(settings: Settings, args) -> int:
    from storage.parquet_migration import migrate_parquet_to_bin

    try:
        stats = migrate_parquet_to_bin(
            settings.data_dir,
            settings.db_path,
            dry_run=args.dry_run,
            delete_parquet=not args.keep_parquet,
        )
    except ImportError:
        print("error: pyarrow is required. Run: pip install pyarrow", file=sys.stderr)
        return 2

    label = "would convert" if args.dry_run else "converted"
    if stats["found"] > 0:
        print(
            f"\n{label} {stats['converted']} file(s), "
            f"skipped {stats['skipped']}, "
            f"deleted {stats['deleted']} parquet, "
            f"{stats['ticks']:,} ticks, "
            f"{stats['paths_updated']} ledger path(s) updated",
        )
    else:
        print("No .parquet files found — nothing to migrate.")
    return 0


def cmd_web(args) -> int:
    import logging

    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    port = _resolve_web_port(args.host, args.port)
    print(f"Web UI: {{http://{args.host}}}:{port}")
    uvicorn.run("web.app:app", host=args.host, port=port, reload=False)
    return 0


def cmd_status(settings: Settings, catalog: InstrumentCatalog, args) -> int:
    instrument = catalog.get(args.symbol)
    db = MetadataDB(settings.db_path)
    summary = db.summary(instrument.id)
    if not summary["by_status"]:
        print(f"No data recorded for {instrument.symbol} yet.")
        return 0
    print(f"{instrument.symbol} ({instrument.name})")
    print(f"  recorded range: {summary['first_hour']} -> {summary['last_hour']} UTC")
    for status, info in sorted(summary["by_status"].items()):
        line = f"  {status:<10} {info['hours']:>8} hours"
        if status == "completed":
            line += f"  ({info['ticks']:,} ticks)"
        print(line)
    return 0


def main(argv: list[str] | None = None) -> int:
    # Windows consoles often default to a legacy codepage (e.g. cp1250) that
    # cannot print some instrument descriptions.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    args = _build_parser().parse_args(argv)
    settings = Settings()
    settings.ensure_directories()
    catalog = InstrumentCatalog(settings.instruments_file)

    try:
        if args.command == "search":
            return cmd_search(catalog, args)
        if args.command == "download":
            return cmd_download(settings, catalog, args)
        if args.command == "gaps":
            return cmd_gaps(settings, catalog, args)
        if args.command == "status":
            return cmd_status(settings, catalog, args)
        if args.command == "migrate":
            return cmd_migrate(settings, args)
        if args.command == "web":
            return cmd_web(args)
    except UnknownInstrumentError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nInterrupted. Progress is saved; rerun the same command to resume.")
        return 130
    return 0
