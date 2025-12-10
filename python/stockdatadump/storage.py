import io
import zipfile
from pathlib import Path
from typing import Iterable, Literal, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
import zstandard as zstd

Format = Literal["parquet", "feather"]


def _detect_format(sample: str) -> str:
    head = sample.lstrip()
    if head.startswith("{") or head.startswith("["):
        return "json"
    return "csv"

def _decompress_zst(path: Path) -> bytes:
    with open(path, "rb") as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            return reader.read()


def dump_to_dataframe(path: Path, fmt_hint: Optional[str] = None) -> pd.DataFrame:
    """Load a single .zst dump into a DataFrame using pandas."""
    raw = _decompress_zst(path)
    text = raw.decode("utf-8", errors="ignore")
    fmt = fmt_hint or _detect_format(text[:1024])

    if fmt == "json":
        return pd.read_json(io.StringIO(text), lines=True)
    return pd.read_csv(io.StringIO(text))


def dumps_to_table(paths: Iterable[Path], fmt_hint: Optional[str] = None) -> pa.Table:
    frames = [dump_to_dataframe(path, fmt_hint=fmt_hint) for path in paths]
    combined = pd.concat(frames, ignore_index=True)
    return pa.Table.from_pandas(combined, preserve_index=False)

def stooq_zip_to_table(dump: Path) -> pa.Table:
    """Convert a Stooq .zst (containing the stooq zip) into an Arrow table."""
    raw = _decompress_zst(dump)
    dfs: list[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if not (lower.endswith(".csv") or lower.endswith(".txt")):
                continue
            with zf.open(name) as fh:
                try:
                    df = pd.read_csv(fh)
                except Exception:
                    continue
                # add symbol from filename
                stem = Path(name).stem
                df["Symbol"] = stem.split(".")[0].upper()
                dfs.append(df)
    if not dfs:
        return pa.Table.from_pandas(pd.DataFrame(), preserve_index=False)
    combined = pd.concat(dfs, ignore_index=True)
    return pa.Table.from_pandas(combined, preserve_index=False)


def write_arrow_table(
    table: pa.Table,
    output: Path,
    format: Format = "parquet",
    compression: str = "zstd",
    use_dictionary: bool = True,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    if format == "feather":
        feather.write_feather(table, output, compression=compression, compression_level=8)
    else:
        pq.write_table(
            table,
            output,
            compression=compression,
            use_dictionary=use_dictionary,
            write_statistics=True,
        )
    return output


def squeeze_numeric(table: pa.Table) -> pa.Table:
    """Downcast numeric columns to reduce file size without losing precision for ints."""
    pandas_df = table.to_pandas()
    for col in pandas_df.columns:
        if pd.api.types.is_integer_dtype(pandas_df[col]):
            pandas_df[col] = pd.to_numeric(pandas_df[col], downcast="integer")
        elif pd.api.types.is_float_dtype(pandas_df[col]):
            # Avoid lossy compression; stay at float64 for safety
            pandas_df[col] = pandas_df[col].astype(np.float64)
    return pa.Table.from_pandas(pandas_df, preserve_index=False)
