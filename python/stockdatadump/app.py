import datetime as dt
import subprocess
from pathlib import Path
from typing import List, Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

# Support running as `python app.py` without installing the package
if __package__ is None or __package__ == "":
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    __package__ = "stockdatadump"

from . import jobs, storage

app = typer.Typer(add_completion=False)
console = Console()


def project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / "rust-core"
        if candidate.exists():
            return parent
    # Fallback to module parent (best effort)
    return here.parent.parent


def ensure_rust_binary(release: bool = True) -> Path:
    """Build the Rust hot path if it is missing."""
    root = project_root().joinpath("rust-core")
    if not root.exists():
        raise RuntimeError(f"rust-core not found at {root}. Run from repo root or set working dir correctly.")
    target = root / "target" / ("release" if release else "debug") / "dump-core"
    if target.exists():
        return target
    console.print("[cyan]building Rust fetcher...[/cyan]")
    cmd = ["cargo", "build"]
    if release:
        cmd.append("--release")
    subprocess.run(cmd, cwd=root, check=True)
    if not target.exists():
        raise RuntimeError("failed to build Rust binary")
    return target


@app.command()
def manifest(
    tickers: List[str] = typer.Argument(..., help="Ticker symbols, e.g. AAPL MSFT SPY"),
    output: Path = typer.Option(
        Path("dumps/manifests/yahoo.jsonl"), "--output", "-o", help="Where to write the manifest"
    ),
    start: Optional[str] = typer.Option(None, "--start", help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="End date YYYY-MM-DD"),
    interval: str = typer.Option("1d", "--interval", help="Yahoo Finance interval, e.g. 1d/1wk/1mo"),
    crumb: Optional[str] = typer.Option(None, "--crumb", help="Yahoo Finance crumb (required)"),
    cookie: Optional[str] = typer.Option(None, "--cookie", help="Yahoo Finance cookie header value (required)"),
):
    """Create a JSONL manifest for the Rust fetcher."""
    start_date = dt.datetime.strptime(start, "%Y-%m-%d").date() if start else None
    end_date = dt.datetime.strptime(end, "%Y-%m-%d").date() if end else None
    jobs.write_manifest(
        tickers, output=output, start=start_date, end=end_date, interval=interval, crumb=crumb, cookie=cookie
    )


@app.command("manifest-stooq")
def manifest_stooq(
    output: Path = typer.Option(Path("dumps/manifests/stooq.jsonl"), "--output", "-o"),
    url: str = typer.Option("https://stooq.pl/db/h/s/i/daily_csv.zip", "--url"),
):
    """Create manifest for Stooq daily bulk zip (no key/auth)."""
    jobs.write_stooq_manifest(output=output, url=url)


@app.command()
def fetch(
    manifest: Path = typer.Option(Path("dumps/manifests/yahoo.jsonl"), "--manifest", "-m"),
    output_dir: Path = typer.Option(Path("dumps/raw"), "--output-dir", "-o"),
    concurrency: int = typer.Option(8, "--concurrency", "-c"),
    retries: int = typer.Option(2, "--retries", "-r"),
    timeout_secs: int = typer.Option(15, "--timeout-secs", "-t"),
    compression_level: int = typer.Option(3, "--level", "-l", help="zstd level (-7..22)"),
):
    """Run the Rust fetcher against a manifest."""
    binary = ensure_rust_binary()
    cmd = [
        str(binary),
        "--concurrency",
        str(concurrency),
        "--retries",
        str(retries),
        "--timeout-secs",
        str(timeout_secs),
        "batch",
        "--manifest",
        str(manifest),
        "--output-dir",
        str(output_dir),
        "--level",
        str(compression_level),
    ]
    console.print(f"[green]running:[/green] {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:  # surface Rust errors without Python traceback
        raise typer.Exit(code=exc.returncode)


@app.command()
def convert(
    dumps_dir: Path = typer.Option(Path("dumps/raw"), "--dumps-dir", "-d"),
    output: Path = typer.Option(Path("dumps/arrow/dump.parquet"), "--output", "-o"),
    format: storage.Format = typer.Option("parquet", "--format", "-f", case_sensitive=False),
    compression: str = typer.Option("zstd", "--compression", "-c"),
    fmt_hint: Optional[str] = typer.Option(None, "--hint", help="csv/json if autodetect fails"),
):
    """Convert compressed dumps into a single parquet/feather file."""
    paths = sorted(dumps_dir.glob("*.zst"))
    if not paths:
        raise typer.BadParameter(f"No .zst files found in {dumps_dir}")
    table = storage.dumps_to_table(paths, fmt_hint=fmt_hint)
    table = storage.squeeze_numeric(table)
    storage.write_arrow_table(table, output=output, format=format, compression=compression)
    console.print(f"[green]arrow file written:[/green] {output}")


@app.command()
def convert_stooq(
    dump: Path = typer.Option(Path("dumps/raw/stooq-daily.zst"), "--dump", "-d"),
    output: Path = typer.Option(Path("dumps/arrow/stooq.parquet"), "--output", "-o"),
    compression: str = typer.Option("zstd", "--compression", "-c"),
):
    """Convert a Stooq bulk dump (.zst containing the zip) into parquet."""
    table = storage.stooq_zip_to_table(dump)
    table = storage.squeeze_numeric(table)
    storage.write_arrow_table(table, output=output, format="parquet", compression=compression)
    console.print(f"[green]arrow file written:[/green] {output}")


@app.command()
def head(
    dump: Path = typer.Argument(..., help="Path to a single .zst dump"),
    rows: int = typer.Option(5, "--rows", "-n"),
    fmt_hint: Optional[str] = typer.Option(None, "--hint", help="csv/json if autodetect fails"),
):
    """Preview a compressed dump quickly."""
    df = storage.dump_to_dataframe(dump, fmt_hint=fmt_hint).head(rows)
    _print_df(df)


def _print_df(df: pd.DataFrame):
    table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        table.add_column(str(col))
    for _, row in df.iterrows():
        table.add_row(*[str(val) for val in row.values])
    console.print(table)


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 1:
        app(["--help"])
    else:
        app()
