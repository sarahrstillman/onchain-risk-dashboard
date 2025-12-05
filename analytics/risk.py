import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

def get_metrics():
    query = """
    SELECT
      wallet_address,
      COUNT(*) AS tx_count_30d,
      SUM(value_eth) AS volume_30d,
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

    df["risk_score"] = 0.6 * df["z_volume"] + 0.4 * df["z_txs"]
    return df
