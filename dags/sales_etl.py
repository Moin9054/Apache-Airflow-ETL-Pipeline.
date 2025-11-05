from __future__ import annotations
from datetime import datetime
from pathlib import Path
import re, io, csv, sqlite3
import pandas as pd
from airflow.decorators import dag, task

DATA_DIR   = Path("/opt/airflow/data")
INPUT_FILE = DATA_DIR / "input" / "sales.csv"

TMP_DIR = Path("/tmp/airflow_etl")
DB_FILE = TMP_DIR / "warehouse.db"
def _canon(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())

def _decode_best(path: Path, forced: str | None = None) -> tuple[str, str]:
    data = path.read_bytes()
    tries = ([forced] if forced else []) + ["utf-8-sig", "utf-16", "utf-16le", "utf-16be", "latin1"]
    last_err = None
    for enc in [e for e in tries if e]:
        try:
            txt = data.decode(enc)
            print(f"[decode] success encoding='{enc}'")
            return txt, enc
        except Exception as e:
            last_err = e
            print(f"[decode] failed encoding='{enc}': {e}")
    txt = data.decode("latin1", errors="replace")
    print("[decode] fallback latin1 with replacement chars")
    return txt, "latin1"

def _normalize_text(txt: str) -> str:
    txt = txt.replace("“", '"').replace("”", '"').replace("„", '"')
    txt = txt.replace("‘", "'").replace("’", "'")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    return txt

def _choose_delimiter(sample: str, forced: str | None = None) -> str:
    if forced:
        print(f"[delim] forced delimiter='{forced}'")
        return forced
    counts = {",": sample.count(","), ";": sample.count(";"), "\t": sample.count("\t"), "|": sample.count("|")}
    delim = max(counts, key=count.sget) if False else max(counts, key=counts.get)
    label = "TAB" if delim == "\t" else delim
    print(f"[delim] counts={counts} -> chosen '{label}'")
    return delim

def _sniff_delimiter(sample: str) -> str | None:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        print(f"[sniffer] detected delimiter='{dialect.delimiter}'")
        return dialect.delimiter
    except Exception as e:
        print(f"[sniffer] failed: {e}")
        return None

def robust_read(path: Path, enc_force: str | None, sep_force: str | None, quote_force: str | None) -> pd.DataFrame:
    txt, enc = _decode_best(path, enc_force)
    txt = _normalize_text(txt)
    sample = txt[:5000]

    if sep_force is None and quote_force is None:
        try:
            df = pd.read_csv(io.StringIO(txt), engine="python", sep=None)
            print(f"[read] autodetect sep=None -> shape={df.shape}")
            return df
        except Exception as e:
            print(f"[read] autodetect failed: {e}")

    sep = sep_force or _sniff_delimiter(sample) or _choose_delimiter(sample, sep_force)

    if quote_force:
        quoting = {"none": csv.QUOTE_NONE, "minimal": csv.QUOTE_MINIMAL}.get(quote_force.lower(), csv.QUOTE_NONE)
    else:
        quoting = csv.QUOTE_NONE

    try:
        df = pd.read_csv(
            io.StringIO(txt),
            engine="python",
            sep=sep,
            quoting=quoting,
            quotechar='"',
            escapechar="\\",
            on_bad_lines="skip",
        )
        print(f"[read] sep='{sep if sep!='\\t' else 'TAB'}' quoting={quoting} -> shape={df.shape}")
        return df
    except Exception as e:
        print(f"[read] tolerant read failed: {e}")

    try:
        df = pd.read_csv(
            io.StringIO(txt),
            engine="python",
            sep=r"[,\t;|]",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            on_bad_lines="skip",
        )
        print(f"[read] regex multi-delimiter -> shape={df.shape}")
        return df
    except Exception as e:
        print(f"[read] regex failed: {e}")
        raise RuntimeError("[read] Could not parse CSV even with robust strategies")
@dag(
    dag_id="sales_csv_to_sqlite",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "pandas", "sqlite"],
)
def sales_csv_to_sqlite():

    @task
    def extract() -> str:
        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"[extract] Missing input file: {INPUT_FILE}")
        print(f"[extract] Using CSV: {INPUT_FILE}")
        return str(INPUT_FILE)

    @task
    def transform(csv_path: str) -> str:
        enc_force = None               
        sep_force = None               
        quote_force = None

        print(f"[transform] csv='{csv_path}' enc_force={enc_force} sep_force={sep_force} quote_force={quote_force}")
        df = robust_read(Path(csv_path), enc_force, sep_force, quote_force)

        print(f"[transform] original columns -> {list(df.columns)}")
        df.columns = [c.strip() for c in df.columns]
        canon_map = {_canon(c): c for c in df.columns}

        required_variants = {
            "date":     ["date"],
            "order_id": ["order_id", "order id", "orderid"],
            "item":     ["item", "product", "sku", "name"],
            "qty":      ["qty", "quantity", "units", "count"],
            "price":    ["price", "unitprice", "amount", "rate"],
        }
        rename = {}
        for target, variants in required_variants.items():
            found = None
            for v in variants:
                k = _canon(v)
                if k in canon_map:
                    found = canon_map[k]
                    break
            if found:
                rename[found] = target
        df = df.rename(columns=rename)

        required = {"date", "order_id", "item", "qty", "price"}
        missing  = required - set(df.columns)
        if missing:
            print("[transform] preview head(5):")
            try:
                print(df.head().to_string())
            except Exception:
                pass
            raise ValueError(f"[transform] missing columns: {missing}; seen: {list(df.columns)}")

        df["date"]  = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["qty"]   = pd.to_numeric(df["qty"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        before = len(df)
        df = df.dropna(subset=["date", "order_id", "item", "qty", "price"])
        df = df[df["qty"] > 0]
        print(f"[transform] dropped {before - len(df)} rows; keep {len(df)}")

        df["revenue"] = df["qty"] * df["price"]
        agg = (
            df.groupby("date", as_index=False)
              .agg(total_orders=("order_id","nunique"),
                   total_qty=("qty","sum"),
                   total_revenue=("revenue","sum"))
        )

        TMP_DIR.mkdir(parents=True, exist_ok=True)
        fact_path = TMP_DIR / "tmp_fact.csv"
        agg_path  = TMP_DIR / "tmp_agg.csv"
        df.to_csv(fact_path, index=False)
        agg.to_csv(agg_path, index=False)
        print(f"[transform] wrote facts -> {fact_path}; agg -> {agg_path}")
        return f"{fact_path}|{agg_path}"

    @task
    def load(paths: str) -> str:
        fact_path, agg_path = paths.split("|")
        print(f"[load] reading: {fact_path} & {agg_path}")
        fact_df = pd.read_csv(fact_path)
        agg_df  = pd.read_csv(agg_path)
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_FILE) as con:
            fact_df.to_sql("fact_sales", con, if_exists="append", index=False)
            agg_df.to_sql("agg_daily_sales", con, if_exists="replace", index=False)
        msg = f"[load] loaded {len(fact_df)} fact and {len(agg_df)} agg rows into {DB_FILE}"
        print(msg)
        return msg

    @task
    def export_report() -> str:
        out = DATA_DIR / "output" / "daily_sales_report.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_FILE) as con:
            df = pd.read_sql("select * from agg_daily_sales order by date", con)
        df.to_csv(out, index=False)
        print(f"[report] wrote {out}")
        return str(out)

    p = transform(extract())
    load(p) >> export_report()

sales_csv_to_sqlite()
