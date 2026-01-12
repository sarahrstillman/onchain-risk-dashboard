import os
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))


def build_daily_metrics(large_tx_threshold: float = 1000.0) -> pd.DataFrame:
    metrics: List[Dict[str, Any]] = []

    entity_query = """
    WITH entity_txs AS (
        SELECT
            date(t.timestamp) AS metric_date,
            e.entity_type AS entity_type,
            e.label AS entity_label,
            CASE WHEN LOWER(t.to_address) = e.address THEN t.value_eth ELSE 0 END AS inflow,
            CASE WHEN LOWER(t.from_address) = e.address THEN t.value_eth ELSE 0 END AS outflow
        FROM transactions t
        JOIN entities e
          ON LOWER(t.to_address) = e.address OR LOWER(t.from_address) = e.address
        WHERE t.value_eth IS NOT NULL
    )
    SELECT
        metric_date,
        entity_type,
        entity_label,
        SUM(inflow) AS inflow,
        SUM(outflow) AS outflow,
        SUM(inflow) - SUM(outflow) AS net_flow
    FROM entity_txs
    GROUP BY metric_date, entity_type, entity_label;
    """
    entity_df = pd.read_sql(entity_query, engine)
    if not entity_df.empty:
        for _, row in entity_df.iterrows():
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "inflow",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": "ETH",
                    "value": row["inflow"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "outflow",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": "ETH",
                    "value": row["outflow"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "net_flow",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": "ETH",
                    "value": row["net_flow"],
                }
            )

    large_tx_query = """
    SELECT
        date(timestamp) AS metric_date,
        COUNT(*) AS large_tx_count,
        SUM(value_eth) AS large_tx_volume
    FROM transactions
    WHERE value_eth >= ?
    GROUP BY date(timestamp);
    """
    large_df = pd.read_sql(large_tx_query, engine, params=[large_tx_threshold])
    if not large_df.empty:
        for _, row in large_df.iterrows():
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "large_tx_count",
                    "entity_type": None,
                    "entity_label": None,
                    "asset_symbol": "ETH",
                    "value": row["large_tx_count"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "large_tx_volume",
                    "entity_type": None,
                    "entity_label": None,
                    "asset_symbol": "ETH",
                    "value": row["large_tx_volume"],
                }
            )

    return pd.DataFrame(metrics)


def write_daily_metrics(df: pd.DataFrame) -> None:
    if df.empty:
        return
    df.to_sql("daily_metrics", engine, if_exists="append", index=False)
