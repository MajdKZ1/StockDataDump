import datetime as dt
import json
import os
from pathlib import Path
from typing import Iterable, List, Optional

from rich.console import Console

console = Console()


def _date_to_epoch(date: dt.date) -> int:
    return int(dt.datetime(date.year, date.month, date.day, tzinfo=dt.timezone.utc).timestamp())


def yahoo_history_url(symbol: str, start: dt.date, end: dt.date, interval: str = "1d", crumb: str | None = None) -> str:
    """Build a Yahoo Finance download URL (crumb required for auth)."""
    period1 = _date_to_epoch(start)
    period2 = _date_to_epoch(end)
    crumb_part = f"&crumb={crumb}" if crumb else ""
    return (
        f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
        f"?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true{crumb_part}"
    )


def write_manifest(
    tickers: Iterable[str],
    output: Path,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
    interval: str = "1d",
    crumb: str | None = None,
    cookie: str | None = None,
) -> Path:
    """Create a JSONL manifest compatible with the Rust fetcher."""
    start = start or (dt.date.today() - dt.timedelta(days=365))
    end = end or dt.date.today()

    # Allow env-based defaults so users can set once
    crumb = crumb or os.getenv("YAHOO_CRUMB")
    cookie = cookie or os.getenv("YAHOO_COOKIE")

    output.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    for ticker in tickers:
        url = yahoo_history_url(ticker, start, end, interval, crumb=crumb)
        entry: dict[str, object] = {"symbol": ticker, "url": url}
        if cookie:
            entry["headers"] = {"cookie": cookie}
        lines.append(json.dumps(entry))

    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    console.print(f"[green]manifest written:[/green] {output}")
    return output


def write_stooq_manifest(
    output: Path = Path("dumps/manifests/stooq.jsonl"),
    url: str = "https://stooq.pl/db/h/s/i/daily_csv.zip",
) -> Path:
    """Manifest pointing at the Stooq daily bulk zip (no auth/key needed)."""
    output.parent.mkdir(parents=True, exist_ok=True)
    entry = {"symbol": "stooq-daily", "url": url}
    output.write_text(json.dumps(entry) + "\n", encoding="utf-8")
    console.print(f"[green]manifest written:[/green] {output} -> {url}")
    return output
