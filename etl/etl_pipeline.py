import duckdb
import pandas as pd
RAW_PATH = "data/sales_raw.csv"
DB_PATH = "analytics.duckdb"
TABLE = "sales"


def read():
    df = pd.read_csv(RAW_PATH)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = (df
          .drop_duplicates(subset=["id"], keep="last")
          .assign(ts=lambda d: pd.to_datetime(d["timestamp"], utc=True))
          )
    # Reglas de calidad (hard-fail):
    assert df["price"].isna().mean() < 0.05, ">5% de price nulo"
    assert (df["qty"] > 0).all(), "qty inválido"
    assert df["id"].is_unique, "id no es único"
    return df


def load(df: pd.DataFrame):
    con = duckdb.connect(DB_PATH)
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT * FROM df")
    # idempotencia por reemplazo (simple para demo)
    con.execute(f"DELETE FROM {TABLE}")
    con.execute(f"INSERT INTO {TABLE} SELECT * FROM df")


def run():
    df = read()
    df = transform(df)
    load(df)
    print(f"OK -> {DB_PATH}::{TABLE}, rows={len(df)}")


if __name__ == "__main__":
    run()
