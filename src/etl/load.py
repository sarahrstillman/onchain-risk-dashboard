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

    df["value_eth"] = df["value"].astype(float) / 1e18
    df["timestamp"] = pd.to_datetime(df["timeStamp"].astype(int), unit="s")

    df["direction"] = df.apply(
        lambda row: "in" if row["to"].lower() == wallet.lower() else "out",
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
        "token_symbol": None,
        "token_value": None,
        "is_contract_interaction": None
    })

def load_transactions(df):
    if df.empty:
        return
    df.to_sql("transactions", engine, if_exists="append", index=False)
