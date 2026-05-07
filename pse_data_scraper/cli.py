"""
Command-line interface for the PSE Data Scraper.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import List, Optional

from pse_data_scraper import __version__
from pse_data_scraper.client import PSEClient
from pse_data_scraper.config import DEFAULT_CONFIG_NAME, load_config, write_default_config
from pse_data_scraper.downloader import download_historical_data
from pse_data_scraper.pipeline import (
    ensure_companies_csv,
    export_prices,
    fetch_financials,
    fetch_fundamentals,
    fetch_usdphp_fx,
    sync_data,
)
from pse_data_scraper.status import collect_status


def _parse_symbols(value: Optional[str]) -> Optional[List[str]]:
    if value is None:
        return None
    items = [item.strip().upper() for item in value.split(",") if item.strip()]
    return items or None


def _setup_logging(verbose: bool, quiet: bool) -> None:
    if quiet:
        level = logging.WARNING
    else:
        level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def _load_config(path: Optional[str]):
    try:
        return load_config(path)
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        raise SystemExit(2) from exc
    except Exception as exc:  # pragma: no cover - unexpected config errors
        logging.error("Failed to load config: %s", exc)
        raise SystemExit(2) from exc


def _apply_overrides(config, args):
    cfg = replace(config)

    data_dir = getattr(args, "data_dir", None)
    if data_dir:
        cfg.data_dir = Path(data_dir)
        if getattr(args, "companies", None) is None:
            cfg.companies_csv = None
        if getattr(args, "history_dir", None) is None:
            cfg.history_dir = None
        if getattr(args, "combined", None) is None:
            cfg.combined_csv = None

    companies = getattr(args, "companies", None)
    if companies:
        cfg.companies_csv = Path(companies)

    history_dir = getattr(args, "history_dir", None)
    if history_dir:
        cfg.history_dir = Path(history_dir)

    combined = getattr(args, "combined", None)
    if combined:
        cfg.combined_csv = Path(combined)

    cache_dir = getattr(args, "cache_dir", None)
    if cache_dir:
        cfg.cache_dir = Path(cache_dir)

    if getattr(args, "no_cache", False):
        cfg.cache_dir = None

    rate_limit = getattr(args, "rate_limit", None)
    if rate_limit is not None:
        cfg.rate_limit = rate_limit

    start_date = getattr(args, "start_date", None)
    if start_date:
        cfg.start_date = start_date

    end_date = getattr(args, "end_date", None)
    if end_date:
        cfg.end_date = end_date

    symbols = getattr(args, "symbols", None)
    if symbols is not None:
        cfg.symbols = _parse_symbols(symbols) or []

    max_companies = getattr(args, "max_companies", None)
    if max_companies is not None:
        cfg.max_companies = max_companies if max_companies > 0 else None

    cfg.resolve_paths()
    return cfg


def _resolve_config(args):
    config = _load_config(getattr(args, "config", None))
    return _apply_overrides(config, args)


def _print_status(status: dict) -> None:
    companies = status["companies"]
    history = status["history"]
    combined = status["combined"]

    if companies["exists"]:
        print(
            f"Companies CSV: {companies['path']} (rows={companies['rows']}, updated={companies['updated']})"
        )
    else:
        print(f"Companies CSV: missing ({companies['path']})")

    if history["exists"]:
        print(f"History dir: {history['path']} (files={history['files']})")
    else:
        print(f"History dir: missing ({history['path']})")

    if combined["exists"]:
        range_text = combined["date_range"] or "unknown"
        print(
            f"Combined CSV: {combined['path']} (rows={combined['rows']}, updated={combined['updated']}, range={range_text})"
        )
    else:
        print(f"Combined CSV: missing ({combined['path']})")


def handle_init(args) -> None:
    path = Path(args.path)
    created = write_default_config(path, force=args.force)
    if created:
        print(f"Created config at {path}")
    else:
        print(f"Config already exists: {path}")


def handle_companies(args) -> None:
    cfg = _resolve_config(args)
    client = PSEClient(rate_limit_seconds=cfg.rate_limit)
    companies = ensure_companies_csv(
        client=client,
        companies_csv=str(cfg.companies_csv),
        refresh=getattr(args, "refresh", False),
        max_pages=getattr(args, "max_pages", None),
    )
    if getattr(args, "list", False):
        for company in companies:
            print(f"{company.stock_symbol}\t{company.company_name}")


def handle_prices(args) -> None:
    cfg = _resolve_config(args)
    client = PSEClient(rate_limit_seconds=cfg.rate_limit)
    companies = ensure_companies_csv(
        client=client,
        companies_csv=str(cfg.companies_csv),
        refresh=getattr(args, "refresh", False),
        max_pages=getattr(args, "max_pages", None),
    )
    download_historical_data(
        client=client,
        companies=companies,
        output_dir=str(cfg.history_dir),
        start_date=cfg.start_date,
        end_date=cfg.end_date or date.today(),
        symbols=cfg.symbols or None,
        max_companies=cfg.max_companies,
        cache_dir=str(cfg.cache_dir) if cfg.cache_dir else None,
        refresh=getattr(args, "refresh", False),
    )


def handle_export(args) -> None:
    cfg = _resolve_config(args)
    if args.format.lower() != "csv":
        logging.error("Only CSV export is supported right now.")
        raise SystemExit(2)
    export_prices(str(cfg.history_dir), str(cfg.combined_csv))


def handle_sync(args) -> None:
    cfg = _resolve_config(args)
    sync_data(
        companies_csv=str(cfg.companies_csv),
        history_dir=str(cfg.history_dir),
        combined_csv=str(cfg.combined_csv),
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        rate_limit_seconds=cfg.rate_limit,
        symbols=cfg.symbols or None,
        max_companies=cfg.max_companies,
        cache_dir=str(cfg.cache_dir) if cfg.cache_dir else None,
        refresh=getattr(args, "refresh", False),
        max_pages=getattr(args, "max_pages", None),
    )


def handle_status(args) -> None:
    cfg = _resolve_config(args)
    status = collect_status(cfg.companies_csv, cfg.history_dir, cfg.combined_csv)
    _print_status(status)


def handle_index(args) -> None:
    cfg = _resolve_config(args)
    index_dir = getattr(args, "index_dir", None) or str(cfg.data_dir / "index")
    fetch_index(
        index_dir=index_dir,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
    )


def handle_fundamentals(args) -> None:
    cfg = _resolve_config(args)
    client = PSEClient(rate_limit_seconds=cfg.rate_limit)
    companies = ensure_companies_csv(
        client=client,
        companies_csv=str(cfg.companies_csv),
        refresh=False,
    )
    fundamentals_csv = getattr(args, "fundamentals", None) or str(cfg.data_dir / "fundamentals.csv")
    fetch_fundamentals(
        client=client,
        companies=companies,
        fundamentals_csv=fundamentals_csv,
        symbols=cfg.symbols or None,
        max_companies=cfg.max_companies,
    )


def handle_financials(args) -> None:
    cfg = _resolve_config(args)
    client = PSEClient(rate_limit_seconds=cfg.rate_limit)
    companies = ensure_companies_csv(
        client=client,
        companies_csv=str(cfg.companies_csv),
        refresh=False,
    )
    financials_csv = getattr(args, "financials", None) or str(cfg.data_dir / "financials.csv")
    fx_csv = getattr(args, "fx_csv", None) or str(cfg.data_dir / "fx" / "usdphp.csv")
    if not Path(fx_csv).exists() and not getattr(args, "skip_fx", False):
        logging.info("FX CSV not found. Downloading BSP USD/PHP rates first...")
        fetch_usdphp_fx(client=client, output_csv=fx_csv)

    fetch_financials(
        client=client,
        companies=companies,
        financials_csv=financials_csv,
        symbols=cfg.symbols or None,
        max_companies=cfg.max_companies,
        from_date=getattr(args, "from_date", None) or "01-01-2010",
        fx_csv=fx_csv,
    )


def handle_fx(args) -> None:
    cfg = _resolve_config(args)
    client = PSEClient(rate_limit_seconds=cfg.rate_limit)
    output_csv = getattr(args, "output", None) or str(cfg.data_dir / "fx" / "usdphp.csv")
    workbook = getattr(args, "workbook", None)
    fetch_usdphp_fx(client=client, output_csv=output_csv, workbook_path=workbook)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pse", description="PSE EDGE data scraper")
    parser.add_argument("--version", action="version", version=f"pse {__version__}")
    parser.add_argument("--config", help="Path to pse.toml (default: ./pse.toml)")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--quiet", action="store_true", help="Only show warnings and errors")

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create a default config file")
    init_parser.add_argument("--path", default=DEFAULT_CONFIG_NAME, help="Config file path")
    init_parser.add_argument("--force", action="store_true", help="Overwrite if it exists")
    init_parser.set_defaults(func=handle_init)

    sync_parser = subparsers.add_parser("sync", help="Refresh companies, prices, and export")
    sync_parser.add_argument("--data-dir", help="Root data directory")
    sync_parser.add_argument("--companies", "--output", dest="companies", help="Companies CSV path")
    sync_parser.add_argument(
        "--history-dir", "--output-dir", dest="history_dir", help="History data directory"
    )
    sync_parser.add_argument("--combined", help="Combined CSV path")
    sync_parser.add_argument("--cache-dir", help="Cache folder")
    sync_parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    sync_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    sync_parser.add_argument("--symbols", help="Comma-separated stock symbols to download")
    sync_parser.add_argument(
        "--from",
        "--start-date",
        dest="start_date",
        help="Start date (YYYY-MM-DD or MM-DD-YYYY)",
    )
    sync_parser.add_argument(
        "--to",
        "--end-date",
        dest="end_date",
        help="End date (YYYY-MM-DD or MM-DD-YYYY)",
    )
    sync_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    sync_parser.add_argument("--max-pages", type=int, help="Limit number of company pages")
    sync_parser.add_argument("--refresh", action="store_true", help="Refresh companies and prices")
    sync_parser.set_defaults(func=handle_sync)

    companies_parser = subparsers.add_parser("companies", help="Refresh or list companies")
    companies_parser.add_argument("--data-dir", help="Root data directory")
    companies_parser.add_argument("--companies", "--output", dest="companies", help="Companies CSV path")
    companies_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    companies_parser.add_argument("--max-pages", type=int, help="Limit number of pages")
    companies_parser.add_argument("--refresh", action="store_true", help="Re-scrape companies")
    companies_parser.add_argument("--list", action="store_true", help="Print the company list")
    companies_parser.set_defaults(func=handle_companies)

    prices_parser = subparsers.add_parser("prices", help="Download historical prices")
    prices_parser.add_argument("--data-dir", help="Root data directory")
    prices_parser.add_argument("--companies", "--input", dest="companies", help="Companies CSV path")
    prices_parser.add_argument(
        "--history-dir", "--output-dir", dest="history_dir", help="History data directory"
    )
    prices_parser.add_argument("--cache-dir", help="Cache folder")
    prices_parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    prices_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    prices_parser.add_argument("--symbols", help="Comma-separated stock symbols to download")
    prices_parser.add_argument(
        "--from",
        "--start-date",
        dest="start_date",
        help="Start date (YYYY-MM-DD or MM-DD-YYYY)",
    )
    prices_parser.add_argument(
        "--to",
        "--end-date",
        dest="end_date",
        help="End date (YYYY-MM-DD or MM-DD-YYYY)",
    )
    prices_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    prices_parser.add_argument("--max-pages", type=int, help="Limit number of company pages")
    prices_parser.add_argument("--refresh", action="store_true", help="Refresh companies and prices")
    prices_parser.set_defaults(func=handle_prices)

    export_parser = subparsers.add_parser("export", help="Export combined dataset")
    export_parser.add_argument("--data-dir", help="Root data directory")
    export_parser.add_argument("--history-dir", dest="history_dir", help="History data directory")
    export_parser.add_argument("--combined", "--output", dest="combined", help="Combined CSV path")
    export_parser.add_argument("--format", default="csv", help="Export format (csv)")
    export_parser.set_defaults(func=handle_export)

    status_parser = subparsers.add_parser("status", help="Show local dataset status")
    status_parser.add_argument("--data-dir", help="Root data directory")
    status_parser.add_argument("--companies", help="Companies CSV path")
    status_parser.add_argument("--history-dir", help="History data directory")
    status_parser.add_argument("--combined", help="Combined CSV path")
    status_parser.set_defaults(func=handle_status)

    fundamentals_parser = subparsers.add_parser(
        "fundamentals", help="Scrape outstanding/listed shares for each company"
    )
    fundamentals_parser.add_argument("--data-dir", help="Root data directory")
    fundamentals_parser.add_argument("--companies", "--input", dest="companies", help="Companies CSV path")
    fundamentals_parser.add_argument("--output", dest="fundamentals", help="Output fundamentals CSV path")
    fundamentals_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    fundamentals_parser.add_argument("--symbols", help="Comma-separated stock symbols")
    fundamentals_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    fundamentals_parser.set_defaults(func=handle_fundamentals)

    financials_parser = subparsers.add_parser(
        "financials", help="Scrape financial-report metrics for value/growth sorting"
    )
    financials_parser.add_argument("--data-dir", help="Root data directory")
    financials_parser.add_argument("--companies", "--input", dest="companies", help="Companies CSV path")
    financials_parser.add_argument("--output", dest="financials", help="Output financials CSV path")
    financials_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    financials_parser.add_argument("--symbols", help="Comma-separated stock symbols")
    financials_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    financials_parser.add_argument(
        "--from-date",
        dest="from_date",
        default="01-01-2010",
        help="Earliest report date to search (MM-DD-YYYY)",
    )
    financials_parser.add_argument(
        "--fx-csv",
        dest="fx_csv",
        help="BSP USD/PHP CSV path used to normalize financials to PHP",
    )
    financials_parser.add_argument(
        "--skip-fx",
        action="store_true",
        help="Do not auto-download BSP FX CSV when missing",
    )
    financials_parser.set_defaults(func=handle_financials)

    fx_parser = subparsers.add_parser("fx", help="Download BSP USD/PHP daily FX rates")
    fx_parser.add_argument("--data-dir", help="Root data directory")
    fx_parser.add_argument("--output", help="Output FX CSV path")
    fx_parser.add_argument("--workbook", help="Optional local/output workbook path (.xlsx)")
    fx_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    fx_parser.set_defaults(func=handle_fx)

    scrape_parser = subparsers.add_parser(
        "scrape", help="Deprecated. Use `pse companies` instead."
    )
    scrape_parser.add_argument("--output", dest="companies", help="Output CSV file")
    scrape_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    scrape_parser.add_argument("--max-pages", type=int, help="Limit number of pages")
    scrape_parser.add_argument("--refresh", action="store_true", help="Re-scrape companies")
    scrape_parser.set_defaults(func=handle_companies)

    download_parser = subparsers.add_parser(
        "download", help="Deprecated. Use `pse prices` instead."
    )
    download_parser.add_argument("--input", dest="companies", help="Input company CSV")
    download_parser.add_argument("--output-dir", dest="history_dir", help="Output folder")
    download_parser.add_argument("--start-date", dest="start_date", help="Start date (MM-DD-YYYY)")
    download_parser.add_argument("--end-date", dest="end_date", help="End date (MM-DD-YYYY)")
    download_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    download_parser.add_argument("--symbols", help="Comma-separated stock symbols to download")
    download_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    download_parser.add_argument("--cache-dir", help="Cache folder")
    download_parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    download_parser.add_argument("--refresh", action="store_true", help="Refresh companies and prices")
    download_parser.set_defaults(func=handle_prices)

    combine_parser = subparsers.add_parser(
        "combine", help="Deprecated. Use `pse export` instead."
    )
    combine_parser.add_argument("--data-dir", dest="history_dir", help="Data folder")
    combine_parser.add_argument("--output", dest="combined", help="Output CSV file")
    combine_parser.add_argument("--format", default="csv", help="Export format (csv)")
    combine_parser.set_defaults(func=handle_export)

    all_parser = subparsers.add_parser(
        "all", help="Deprecated. Use `pse sync` instead."
    )
    all_parser.add_argument("--output", dest="companies", help="Output company CSV")
    all_parser.add_argument("--output-dir", dest="history_dir", help="Data folder")
    all_parser.add_argument("--combined", dest="combined", help="Combined CSV file")
    all_parser.add_argument("--start-date", dest="start_date", help="Start date (MM-DD-YYYY)")
    all_parser.add_argument("--end-date", dest="end_date", help="End date (MM-DD-YYYY)")
    all_parser.add_argument("--rate-limit", type=float, help="Seconds between requests")
    all_parser.add_argument("--symbols", help="Comma-separated stock symbols to download")
    all_parser.add_argument("--max-companies", type=int, help="Limit number of companies")
    all_parser.add_argument("--cache-dir", help="Cache folder")
    all_parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    all_parser.add_argument("--refresh", action="store_true", help="Refresh companies and prices")
    all_parser.set_defaults(func=handle_sync)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    _setup_logging(args.verbose, args.quiet)
    args.func(args)


if __name__ == "__main__":
    main()
