"""
Scrape fundamental data (outstanding shares, listed shares) from PSE EDGE
company stock-data pages.

The data is used to compute market capitalisation (price × outstanding shares),
which is required for the SMB (size) factor in the Fama-French 3-factor model.
"""

from __future__ import annotations

import csv
import logging
import re
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from bs4 import BeautifulSoup

from pse_data_scraper.client import PSEClient
from pse_data_scraper.models import Company, CompanyFundamentals
from pse_data_scraper.scraper import load_companies_from_csv

logger = logging.getLogger(__name__)

STOCK_DATA_URL = "https://edge.pse.com.ph/companyPage/stockData.do"


def _parse_share_count(text: str) -> Optional[int]:
    """Convert a formatted number string like '1,218,679,950' to an integer."""
    cleaned = re.sub(r"[^\d]", "", text.strip())
    if cleaned:
        return int(cleaned)
    return None


def scrape_company_fundamentals(
    client: PSEClient,
    company: Company,
) -> Optional[CompanyFundamentals]:
    """Fetch outstanding and listed shares for a single company."""
    url = f"{STOCK_DATA_URL}?cmpy_id={company.company_id}"
    try:
        response = client.get(
            url,
            headers={"Referer": "https://edge.pse.com.ph/companyDirectory/form.do"},
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to fetch %s (%s): %s", company.stock_symbol, url, exc)
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    outstanding_shares: Optional[int] = None
    listed_shares: Optional[int] = None

    # The page layout pairs <th> labels with adjacent <td> values inside <tr> rows.
    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        for i, cell in enumerate(cells):
            label = cell.get_text(strip=True).lower()
            if label == "outstanding shares" and i + 1 < len(cells):
                outstanding_shares = _parse_share_count(cells[i + 1].get_text())
            elif label == "listed shares" and i + 1 < len(cells):
                listed_shares = _parse_share_count(cells[i + 1].get_text())

    if outstanding_shares is None and listed_shares is None:
        logger.warning("No share data found for %s", company.stock_symbol)
        return None

    return CompanyFundamentals(
        company_id=company.company_id,
        stock_symbol=company.stock_symbol,
        company_name=company.company_name,
        outstanding_shares=outstanding_shares,
        listed_shares=listed_shares,
    )


def scrape_fundamentals(
    client: PSEClient,
    companies: Sequence[Company],
    symbols: Optional[Sequence[str]] = None,
    max_companies: Optional[int] = None,
) -> List[CompanyFundamentals]:
    """Scrape fundamental data for *companies*, optionally filtered by *symbols*."""
    symbol_set = {s.strip().upper() for s in symbols} if symbols else None
    results: List[CompanyFundamentals] = []
    processed = 0

    for company in companies:
        if symbol_set and company.stock_symbol.upper() not in symbol_set:
            continue
        if max_companies is not None and processed >= max_companies:
            break

        processed += 1
        logger.info(
            "[%d] %s %s %s",
            processed,
            company.stock_symbol,
            company.company_id,
            company.company_name,
        )
        result = scrape_company_fundamentals(client, company)
        if result is not None:
            results.append(result)

    return results


def save_fundamentals_csv(
    rows: Iterable[CompanyFundamentals],
    output_path: str | Path,
) -> Path:
    """Write fundamentals data to *output_path* as CSV."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["companyId", "stockSymbol", "companyName", "outstandingShares", "listedShares"]
        )
        for row in rows:
            writer.writerow(
                [
                    row.company_id,
                    row.stock_symbol,
                    row.company_name,
                    row.outstanding_shares if row.outstanding_shares is not None else "",
                    row.listed_shares if row.listed_shares is not None else "",
                ]
            )

    logger.info("Saved fundamentals to %s", path)
    return path


def download_fundamentals(
    client: PSEClient,
    input_csv: Optional[str] = None,
    companies: Optional[Sequence[Company]] = None,
    output_path: str = "data/fundamentals.csv",
    symbols: Optional[Sequence[str]] = None,
    max_companies: Optional[int] = None,
) -> Path:
    """Download company fundamentals and write to CSV.

    Either *input_csv* or *companies* must be supplied.
    """
    if companies is None:
        if input_csv is None:
            raise ValueError("input_csv is required when companies is not provided")
        companies = load_companies_from_csv(input_csv)

    rows = scrape_fundamentals(
        client=client,
        companies=companies,
        symbols=symbols,
        max_companies=max_companies,
    )
    return save_fundamentals_csv(rows, output_path)
