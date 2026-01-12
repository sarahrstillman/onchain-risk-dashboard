import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import date
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))

def get_metrics():
    query = """
    SELECT
      wallet_address,
      COUNT(*) AS tx_count_30d,
      SUM(value_eth) AS volume_30d,
      COUNT(DISTINCT CASE WHEN direction = 'out' THEN to_address ELSE from_address END)
          AS unique_counterparties_30d,
      AVG(value_eth) AS avg_tx_size
    FROM transactions
    WHERE timestamp >= datetime('now', '-30 days')
    GROUP BY wallet_address;
    """
    return pd.read_sql(query, engine)

def add_risk_scores(df):
    df = df.copy()

    df["z_volume"] = (df["volume_30d"] - df["volume_30d"].mean()) / df["volume_30d"].std()
    df["z_txs"] = (df["tx_count_30d"] - df["tx_count_30d"].mean()) / df["tx_count_30d"].std()

    df[["z_volume", "z_txs"]] = df[["z_volume", "z_txs"]].fillna(0)
    df["risk_score"] = 0.6 * df["z_volume"] + 0.4 * df["z_txs"]
    return df

def build_risk_metrics():
    metrics = get_metrics()
    if metrics.empty:
        return metrics
    scored = add_risk_scores(metrics)
    scored["as_of_date"] = date.today().isoformat()
    return scored

def write_risk_metrics(df: pd.DataFrame) -> None:
    if df.empty:
        return
    columns = [
        "wallet_address",
        "as_of_date",
        "tx_count_30d",
        "volume_30d",
        "unique_counterparties_30d",
        "avg_tx_size",
        "risk_score",
    ]
    df[columns].to_sql("risk_metrics", engine, if_exists="append", index=False)
