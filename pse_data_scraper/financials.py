"""
Scrape financial statement metrics from PSE EDGE financial reports.

This module fetches the latest filed financial report per company and extracts
book-equity related fields used by value-vs-growth and HML construction.
"""

from __future__ import annotations

import csv
import logging
import re
from bisect import bisect_right
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup

from pse_data_scraper.client import PSEClient
from pse_data_scraper.fx import load_fx_csv
from pse_data_scraper.models import Company, CompanyFinancials
from pse_data_scraper.scraper import load_companies_from_csv

logger = logging.getLogger(__name__)

FINANCIAL_REPORTS_SEARCH_URL = "https://edge.pse.com.ph/financialReports/search.ax"
DISC_VIEWER_URL = "https://edge.pse.com.ph/openDiscViewer.do"
DOWNLOAD_HTML_URL = "https://edge.pse.com.ph/downloadHtml.do"


def _parse_number(value: str) -> Optional[float]:
    cleaned = value.strip().replace(",", "")
    cleaned = cleaned.replace("(", "-").replace(")", "")
    if not cleaned or cleaned.upper() in {"N/A", "NA", "-"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_edge_numbers_from_search_html(search_html: str) -> List[str]:
    return re.findall(r"openPopup\('([a-f0-9]+)'\)", search_html)


def _resolve_download_file_id(client: PSEClient, edge_no: str) -> Optional[str]:
    response = client.get(
        DISC_VIEWER_URL,
        params={"edge_no": edge_no},
        headers={"Referer": "https://edge.pse.com.ph/financialReports/form.do"},
    )
    response.raise_for_status()
    match = re.search(r"/downloadHtml\.do\?file_id=(\d+)", response.text)
    if not match:
        return None
    return match.group(1)


def _extract_financial_metrics(download_html: str) -> tuple[Optional[str], Optional[str], Optional[float], Optional[float]]:
    soup = BeautifulSoup(download_html, "html.parser")

    period_ended: Optional[str] = None
    currency: Optional[str] = None

    stockholders_equity: Optional[float] = None
    book_value_per_share: Optional[float] = None

    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(" ", strip=True).lower()
        value_text = cells[1].get_text(" ", strip=True)

        if period_ended is None and (
            label.startswith("for the fiscal year ended")
            or label.startswith("for the period ended")
        ):
            period_ended = value_text

        if currency is None and label.startswith("currency"):
            currency = value_text

        if label in {"stockholders' equity", "stockholders' equity - parent"}:
            parsed = _parse_number(value_text)
            if parsed is not None:
                stockholders_equity = parsed

        if label == "book value per share":
            parsed = _parse_number(value_text)
            if parsed is not None:
                book_value_per_share = parsed

    return period_ended, currency, stockholders_equity, book_value_per_share


def _normalize_currency_and_scale(currency: Optional[str]) -> tuple[Optional[str], float]:
    if not currency:
        return currency, 1.0

    cleaned = currency.strip()
    lowered = cleaned.lower()

    scale = 1.0
    if re.search(r"\bin\s+millions?\b", lowered):
        scale = 1_000_000.0
    elif re.search(r"\bin\s+thousands?\b", lowered):
        scale = 1_000.0

    # Remove common unit qualifiers and punctuation wrappers.
    normalized = re.sub(r"\(?\s*in\s+millions?\s*\)?", "", cleaned, flags=re.IGNORECASE)
    normalized = re.sub(r"\(?\s*in\s+thousands?\s*\)?", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s*,\s*", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" -,")

    return normalized, scale


def _currency_code(currency: Optional[str]) -> Optional[str]:
    if not currency:
        return None
    token = currency.strip().upper()
    lowered = token.lower()

    if "peso" in lowered or "php" in lowered:
        return "PHP"
    if token.startswith("US$"):
        return "USD"
    if token.startswith("PHP"):
        return "PHP"
    if token.startswith("USD"):
        return "USD"
    return token.split()[0] if token else None


def _parse_period_ended(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _resolve_usdphp_rate(target_date: date, rates: Dict[date, float]) -> Optional[Tuple[date, float]]:
    if not rates:
        return None
    ordered_dates = sorted(rates.keys())
    idx = bisect_right(ordered_dates, target_date) - 1
    if idx < 0:
        return None
    dt = ordered_dates[idx]
    return dt, rates[dt]


def scrape_company_financials(
    client: PSEClient,
    company: Company,
    from_date: str,
    to_date: str,
    usdphp_rates: Optional[Dict[date, float]] = None,
) -> Optional[CompanyFinancials]:
    response = client.post(
        FINANCIAL_REPORTS_SEARCH_URL,
        data={
            "companyId": company.company_id,
            "keyword": "",
            "tmplNm": "",
            "fromDate": from_date,
            "toDate": to_date,
        },
        headers={"Referer": f"https://edge.pse.com.ph/financialReports/form.do?cmpy_id={company.company_id}"},
    )
    response.raise_for_status()

    edge_numbers = _extract_edge_numbers_from_search_html(response.text)
    if not edge_numbers:
        logger.warning("No financial reports found for %s", company.stock_symbol)
        return None

    for edge_no in edge_numbers:
        file_id = _resolve_download_file_id(client, edge_no)
        if file_id is None:
            continue

        html_resp = client.get(
            DOWNLOAD_HTML_URL,
            params={"file_id": file_id},
            headers={"Referer": f"https://edge.pse.com.ph/openDiscViewer.do?edge_no={edge_no}"},
        )
        html_resp.raise_for_status()

        period_ended, currency, stockholders_equity, book_value_per_share = _extract_financial_metrics(
            html_resp.text
        )
        if stockholders_equity is None and book_value_per_share is None:
            continue

        normalized_currency, scale = _normalize_currency_and_scale(currency)
        normalized_currency_code = _currency_code(normalized_currency)
        report_date = _parse_period_ended(period_ended)

        if normalized_currency_code == "PHP":
            normalized_currency = "PHP"

        if stockholders_equity is not None:
            stockholders_equity *= scale
        if normalized_currency_code == "USD":
            if report_date is None:
                logger.warning(
                    "Cannot convert USD values to PHP for %s: missing/invalid period ended",
                    company.stock_symbol,
                )
                continue
            if not usdphp_rates:
                logger.warning(
                    "Cannot convert USD values to PHP for %s: no BSP FX rates loaded",
                    company.stock_symbol,
                )
                continue

            resolved = _resolve_usdphp_rate(report_date, usdphp_rates)
            if resolved is None:
                logger.warning(
                    "Cannot convert USD values to PHP for %s: no BSP rate on/before %s",
                    company.stock_symbol,
                    report_date,
                )
                continue

            _, usd_php = resolved
            if stockholders_equity is not None:
                stockholders_equity *= usd_php
            if book_value_per_share is not None:
                book_value_per_share *= usd_php
            normalized_currency = "PHP"

        return CompanyFinancials(
            company_id=company.company_id,
            stock_symbol=company.stock_symbol,
            company_name=company.company_name,
            edge_no=edge_no,
            period_ended=period_ended,
            currency=normalized_currency,
            stockholders_equity=stockholders_equity,
            book_value_per_share=book_value_per_share,
        )

    logger.warning("No parseable financial metrics for %s", company.stock_symbol)
    return None


def scrape_financials(
    client: PSEClient,
    companies: Sequence[Company],
    symbols: Optional[Sequence[str]] = None,
    max_companies: Optional[int] = None,
    from_date: str = "01-01-2010",
    to_date: Optional[str] = None,
    usdphp_rates: Optional[Dict[date, float]] = None,
) -> List[CompanyFinancials]:
    symbol_set = {s.strip().upper() for s in symbols} if symbols else None
    stop_at = max_companies if max_companies is not None else len(companies)
    date_to = to_date or date.today().strftime("%m-%d-%Y")

    rows: List[CompanyFinancials] = []
    processed = 0

    for company in companies:
        if symbol_set and company.stock_symbol.upper() not in symbol_set:
            continue
        if processed >= stop_at:
            break

        processed += 1
        logger.info("[%d] %s %s", processed, company.stock_symbol, company.company_name)
        item = scrape_company_financials(
            client,
            company,
            from_date=from_date,
            to_date=date_to,
            usdphp_rates=usdphp_rates,
        )
        if item is not None:
            rows.append(item)

    return rows


def save_financials_csv(rows: Iterable[CompanyFinancials], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "companyId",
                "stockSymbol",
                "companyName",
                "edgeNo",
                "periodEnded",
                "currency",
                "stockholdersEquity",
                "bookValuePerShare",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.company_id,
                    row.stock_symbol,
                    row.company_name,
                    row.edge_no,
                    row.period_ended or "",
                    row.currency or "",
                    row.stockholders_equity if row.stockholders_equity is not None else "",
                    row.book_value_per_share if row.book_value_per_share is not None else "",
                ]
            )

    logger.info("Saved financials to %s", path)
    return path


def download_financials(
    client: PSEClient,
    input_csv: Optional[str] = None,
    companies: Optional[Sequence[Company]] = None,
    output_path: str = "data/financials.csv",
    symbols: Optional[Sequence[str]] = None,
    max_companies: Optional[int] = None,
    from_date: str = "01-01-2010",
    to_date: Optional[str] = None,
    fx_csv: Optional[str] = None,
) -> Path:
    if companies is None:
        if input_csv is None:
            raise ValueError("input_csv is required when companies is not provided")
        companies = load_companies_from_csv(input_csv)

    usdphp_rates: Optional[Dict[date, float]] = None
    if fx_csv:
        fx_path = Path(fx_csv)
        if fx_path.exists():
            usdphp_rates = load_fx_csv(fx_path)
            logger.info("Loaded BSP USD/PHP rates: %d rows", len(usdphp_rates))
        else:
            logger.warning("FX CSV not found: %s (USD rows may be skipped)", fx_path)

    rows = scrape_financials(
        client=client,
        companies=companies,
        symbols=symbols,
        max_companies=max_companies,
        from_date=from_date,
        to_date=to_date,
        usdphp_rates=usdphp_rates,
    )
    return save_financials_csv(rows, output_path)
