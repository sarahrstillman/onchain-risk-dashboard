import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv("src/config/.env")
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

def normalize(df, wallet):
    if df.empty:
        return df

    df = df.copy()

    has_category = "category" in df.columns
    is_erc20 = df["category"].eq("erc20") if has_category else pd.Series(False, index=df.index)

    if "value_eth" in df.columns:
        df["value_eth"] = pd.to_numeric(df["value_eth"], errors="coerce")
    else:
        df["value_eth"] = pd.to_numeric(df["value"], errors="coerce") / 1e18
    df.loc[is_erc20, "value_eth"] = None

    if "timeStamp" in df.columns:
        raw_ts = df["timeStamp"]
        numeric_ts = pd.to_numeric(raw_ts, errors="coerce")
        # Guard against very large values (e.g., already in ns) that overflow when scaled.
        valid_seconds = numeric_ts.between(0, 253402300799, inclusive="both")
        ts_numeric = pd.to_datetime(
            numeric_ts.where(valid_seconds),
            unit="s",
            errors="coerce",
            utc=True,
        )
        ts_iso = pd.to_datetime(
            raw_ts.where(numeric_ts.isna()),
            errors="coerce",
            utc=True,
            format="ISO8601",
        )
        df["timestamp"] = ts_numeric.fillna(ts_iso)
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    if "blockNumber" in df.columns:
        df["blockNumber"] = df["blockNumber"].apply(
            lambda value: int(value, 16) if isinstance(value, str) and value.startswith("0x") else value
        )

    if has_category and is_erc20.any():
        df["direction"] = None
    else:
        df["direction"] = df.apply(
            lambda row: "in" if str(row["to"]).lower() == wallet.lower() else "out",
            axis=1
        )

    return pd.DataFrame({
        "tx_hash": df["hash"],
        "wallet_address": wallet,
        "direction": df["direction"],
        "from_address": df["from"],
        "to_address": df["to"],
        "value_eth": df["value_eth"],
        "block_number": df["blockNumber"],
        "timestamp": df["timestamp"],
        "token_symbol": df["token_symbol"] if "token_symbol" in df.columns else None,
        "token_value": df["token_value"] if "token_value" in df.columns else None,
        "is_contract_interaction": None
    })

def load_transactions(df):
    if df.empty:
        return
    df.to_sql(
        "transactions",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
