"""
Download and parse BSP USD/PHP daily FX rates.

Source: https://www.bsp.gov.ph/statistics/external/pesodollar.xlsx
"""

from __future__ import annotations

import csv
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from pse_data_scraper.client import PSEClient

logger = logging.getLogger(__name__)

BSP_USDPHP_XLSX_URL = "https://www.bsp.gov.ph/statistics/external/pesodollar.xlsx"

_NS_MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_NS_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_NS_PKG_REL = "{http://schemas.openxmlformats.org/package/2006/relationships}"

_MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _column_index_from_ref(cell_ref: str) -> int:
    letters = ""
    for ch in cell_ref:
        if ch.isalpha():
            letters += ch.upper()
        else:
            break
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx


def _cell_value(cell: ET.Element, shared_strings: List[str]) -> str:
    value_node = cell.find(f"{_NS_MAIN}v")
    if value_node is None or value_node.text is None:
        return ""

    text = value_node.text.strip()
    if cell.attrib.get("t") == "s":
        try:
            return shared_strings[int(text)]
        except (ValueError, IndexError):
            return ""
    return text


def _read_shared_strings(workbook: ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []

    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    out: List[str] = []
    for item in root.findall(f"{_NS_MAIN}si"):
        parts = [node.text or "" for node in item.iter(f"{_NS_MAIN}t")]
        out.append("".join(parts))
    return out


def _resolve_daily_sheet_path(workbook: ZipFile) -> str:
    wb_root = ET.fromstring(workbook.read("xl/workbook.xml"))
    rel_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))

    rel_map: Dict[str, str] = {}
    for rel in rel_root.findall(f"{_NS_PKG_REL}Relationship"):
        rel_id = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        if rel_id and target:
            rel_map[rel_id] = target

    sheets = wb_root.find(f"{_NS_MAIN}sheets")
    if sheets is None:
        raise ValueError("Invalid BSP workbook: missing sheets")

    for sheet in sheets.findall(f"{_NS_MAIN}sheet"):
        name = (sheet.attrib.get("name") or "").strip().lower()
        if name != "daily":
            continue

        rel_id = sheet.attrib.get(f"{_NS_REL}id")
        if not rel_id:
            break

        target = rel_map.get(rel_id)
        if not target:
            break

        if target.startswith("/"):
            return target.lstrip("/")
        return f"xl/{target}" if not target.startswith("xl/") else target

    raise ValueError("BSP workbook does not contain a 'daily' sheet")


def _parse_float(text: str) -> Optional[float]:
    cleaned = text.strip()
    if cleaned in {"", "..", ".", "n.a.", "N.A."}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_daily_sheet_xml(xml_text: bytes, shared_strings: List[str]) -> Dict[date, float]:
    root = ET.fromstring(xml_text)
    rows = root.findall(f".//{_NS_MAIN}sheetData/{_NS_MAIN}row")

    current_year: Optional[int] = None
    month_columns: Dict[int, int] = {}
    rates: Dict[date, float] = {}

    for row in rows:
        cells = row.findall(f"{_NS_MAIN}c")
        if not cells:
            continue

        values_by_col: Dict[int, str] = {}
        for cell in cells:
            ref = cell.attrib.get("r", "")
            if not ref:
                continue
            col = _column_index_from_ref(ref)
            values_by_col[col] = _cell_value(cell, shared_strings)

        first = (values_by_col.get(1) or "").strip()

        # Year marker row in the BSP daily sheet.
        if re.fullmatch(r"\d{4}", first):
            current_year = int(first)
            month_columns = {}
            continue

        # Month header row starts with "Day" then Jan..Dec.
        if first.lower() == "day":
            month_columns = {}
            for col, raw in values_by_col.items():
                token = raw.strip().lower()[:3]
                if token in _MONTH_MAP:
                    month_columns[col] = _MONTH_MAP[token]
            continue

        if current_year is None or not month_columns:
            continue

        if not first.isdigit():
            continue

        day_of_month = int(first)
        for col, month in month_columns.items():
            rate = _parse_float(values_by_col.get(col, ""))
            if rate is None:
                continue
            try:
                dt = date(current_year, month, day_of_month)
            except ValueError:
                continue
            rates[dt] = rate

    return rates


def download_bsp_usdphp_workbook(client: PSEClient, output_path: str | Path) -> Path:
    response = client.get(BSP_USDPHP_XLSX_URL, allow_redirects=True)
    response.raise_for_status()

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return path


def parse_bsp_usdphp_daily_rates(workbook_path: str | Path) -> Dict[date, float]:
    with ZipFile(workbook_path) as workbook:
        shared_strings = _read_shared_strings(workbook)
        daily_sheet_path = _resolve_daily_sheet_path(workbook)
        xml_bytes = workbook.read(daily_sheet_path)
    return _parse_daily_sheet_xml(xml_bytes, shared_strings)


def save_fx_csv(rates: Dict[date, float], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "usdPhp"])
        for dt in sorted(rates):
            writer.writerow([dt.isoformat(), rates[dt]])

    logger.info("Saved BSP USD/PHP daily rates to %s (%d rows)", path, len(rates))
    return path


def load_fx_csv(path: str | Path) -> Dict[date, float]:
    rates: Dict[date, float] = {}
    with Path(path).open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            raw_date = (row.get("date") or "").strip()
            raw_rate = (row.get("usdPhp") or "").strip()
            if not raw_date or not raw_rate:
                continue
            try:
                dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
                rate = float(raw_rate)
            except ValueError:
                continue
            rates[dt] = rate
    return rates


def download_usdphp_fx_csv(
    client: PSEClient,
    output_path: str | Path = "data/fx/usdphp.csv",
    workbook_path: Optional[str | Path] = None,
) -> Path:
    workbook_out = (
        Path(workbook_path)
        if workbook_path is not None
        else Path(output_path).with_suffix(".xlsx")
    )
    downloaded = download_bsp_usdphp_workbook(client=client, output_path=workbook_out)
    rates = parse_bsp_usdphp_daily_rates(downloaded)
    return save_fx_csv(rates=rates, output_path=output_path)
