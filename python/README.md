# StockDataDump Python

Small CLI orchestrator around the Rust fetcher. Install in editable mode:

```bash
# from repo root
pip install -e python
```

Commands:

- `stockdatadump manifest AAPL MSFT --crumb <crumb> --cookie "<cookie>"` – create a Yahoo Finance manifest at `dumps/manifests/yahoo.jsonl` (crumb+cookie required by Yahoo; or set env `YAHOO_CRUMB` / `YAHOO_COOKIE`).
- `stockdatadump fetch --manifest dumps/manifests/yahoo.jsonl --output-dir dumps/raw` – call the Rust binary to pull and compress.
- `stockdatadump convert --dumps-dir dumps/raw --output dumps/arrow/dump.parquet` – combine compressed dumps into Arrow (parquet/feather).
- `stockdatadump head dumps/raw/AAPL.zst` – preview a compressed dump.
