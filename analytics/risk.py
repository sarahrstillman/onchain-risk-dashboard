from datetime import date
import os

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))

PIPELINE_VERSION = "v1.1"
REASON_COLUMNS = [
    "reason_velocity",
    "reason_new_counterparties",
    "reason_contract_interactions",
]


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0, index=series.index)
    return (series - series.mean()) / std

def get_metrics():
    query = """
    SELECT
      wallet_address,
      COUNT(*) AS tx_count_30d,
      SUM(value_eth) AS volume_30d,
      COUNT(DISTINCT CASE WHEN direction = 'out' THEN to_address ELSE from_address END)
          AS unique_counterparties_30d,
      SUM(CASE WHEN is_contract_interaction = 1 THEN 1 ELSE 0 END)
          AS contract_interactions_30d,
      AVG(value_eth) AS avg_tx_size
    FROM transactions
    JOIN entities
      ON LOWER(transactions.wallet_address) = entities.address
    WHERE timestamp >= datetime('now', '-30 days')
      AND LOWER(entities.entity_type) NOT IN ('stablecoin', 'contract', 'bridge', 'erc20')
    GROUP BY wallet_address;
    """
    return pd.read_sql(query, engine)

def add_risk_scores(df):
    df = df.copy()

    df["z_volume"] = _zscore(df["volume_30d"])
    df["z_txs"] = _zscore(df["tx_count_30d"])
    df["z_counterparties"] = _zscore(df["unique_counterparties_30d"])
    df["z_contract_interactions"] = _zscore(df["contract_interactions_30d"])

    df[["z_volume", "z_txs", "z_counterparties", "z_contract_interactions"]] = (
        df[["z_volume", "z_txs", "z_counterparties", "z_contract_interactions"]].fillna(0)
    )
    df["risk_score"] = 0.6 * df["z_volume"] + 0.4 * df["z_txs"]

    df["reason_velocity"] = df["z_txs"].clip(lower=0)
    df["reason_new_counterparties"] = df["z_counterparties"].clip(lower=0)
    df["reason_contract_interactions"] = df["z_contract_interactions"].clip(lower=0)
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
        "contract_interactions_30d",
        "avg_tx_size",
        "risk_score",
        "reason_velocity",
        "reason_new_counterparties",
        "reason_contract_interactions",
    ]
    df[columns].to_sql("risk_metrics", engine, if_exists="append", index=False)
    write_audit_table(df)


def write_audit_table(df: pd.DataFrame) -> None:
    if df.empty:
        return

    def _top_reasons(row: pd.Series) -> str:
        scored = {
            "velocity": row.get("reason_velocity", 0),
            "new_counterparties": row.get("reason_new_counterparties", 0),
            "contract_interactions": row.get("reason_contract_interactions", 0),
        }
        ranked = [
            name for name, score in sorted(scored.items(), key=lambda item: item[1], reverse=True)
            if score and score > 0
        ]
        return ",".join(ranked[:3])

    audit_df = pd.DataFrame({
        "wallet_address": df["wallet_address"],
        "as_of_date": df["as_of_date"],
        "risk_score": df["risk_score"],
        "top_reasons": df.apply(_top_reasons, axis=1),
        "pipeline_version": PIPELINE_VERSION,
    })
    audit_df.to_sql("audit_table", engine, if_exists="append", index=False)
