import os
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


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
    large_df = pd.read_sql(large_tx_query, engine, params=(large_tx_threshold,))
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

    stablecoin_query = """
    SELECT
        date(t.timestamp) AS metric_date,
        e.entity_type AS entity_type,
        e.label AS entity_label,
        t.token_symbol AS asset_symbol,
        SUM(CASE WHEN LOWER(t.from_address) = :zero_address THEN t.token_value ELSE 0 END) AS minted,
        SUM(CASE WHEN LOWER(t.to_address) = :zero_address THEN t.token_value ELSE 0 END) AS burned,
        SUM(CASE WHEN ex_to.address IS NOT NULL THEN t.token_value ELSE 0 END) AS to_exchanges,
        SUM(CASE WHEN ex_from.address IS NOT NULL THEN t.token_value ELSE 0 END) AS from_exchanges,
        COUNT(*) AS transfer_count
    FROM transactions t
    JOIN entities e
      ON LOWER(t.wallet_address) = e.address
    LEFT JOIN entities ex_to
      ON LOWER(t.to_address) = ex_to.address
     AND (LOWER(ex_to.entity_type) LIKE '%exchange%' OR LOWER(ex_to.entity_type) LIKE '%hot%')
    LEFT JOIN entities ex_from
      ON LOWER(t.from_address) = ex_from.address
     AND (LOWER(ex_from.entity_type) LIKE '%exchange%' OR LOWER(ex_from.entity_type) LIKE '%hot%')
    WHERE LOWER(e.entity_type) IN ('stablecoin', 'contract', 'bridge', 'erc20')
      AND t.token_value IS NOT NULL
    GROUP BY metric_date, e.entity_type, e.label, t.token_symbol;
    """
    stable_df = pd.read_sql(stablecoin_query, engine, params={"zero_address": ZERO_ADDRESS})
    if not stable_df.empty:
        stable_df = stable_df.sort_values("metric_date")
        for _, row in stable_df.iterrows():
            net_exchange_flow = row["to_exchanges"] - row["from_exchanges"]
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "tokens_minted",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["minted"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "tokens_burned",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["burned"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "net_flow_to_exchanges",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": net_exchange_flow,
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "transfer_count",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["transfer_count"],
                }
            )

        stable_df["transfer_count"] = pd.to_numeric(stable_df["transfer_count"], errors="coerce").fillna(0)
        stable_df["transfer_count_7d_avg"] = (
            stable_df.groupby(["entity_label", "asset_symbol"])["transfer_count"]
            .rolling(7, min_periods=2)
            .mean()
            .shift(1)
            .reset_index(level=[0, 1], drop=True)
        )
        stable_df["transfer_count_7d_delta"] = (
            stable_df["transfer_count"] - stable_df["transfer_count_7d_avg"]
        )
        for _, row in stable_df.dropna(subset=["transfer_count_7d_delta"]).iterrows():
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "transfer_count_7d_delta",
                    "entity_type": row["entity_type"],
                    "entity_label": row["entity_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["transfer_count_7d_delta"],
                }
            )

    exchange_flow_query = """
    WITH exchange_addresses AS (
        SELECT address, label
        FROM entities
        WHERE LOWER(entity_type) LIKE '%exchange%' OR LOWER(entity_type) LIKE '%hot%'
    ),
    eth_flows AS (
        SELECT
            date(t.timestamp) AS metric_date,
            CASE
                WHEN to_ex.address IS NOT NULL AND from_ex.address IS NULL THEN to_ex.label
                WHEN from_ex.address IS NOT NULL AND to_ex.address IS NULL THEN from_ex.label
            END AS exchange_label,
            'ETH' AS asset_symbol,
            SUM(CASE
                    WHEN to_ex.address IS NOT NULL AND from_ex.address IS NULL
                    THEN t.value_eth ELSE 0 END) AS deposits,
            SUM(CASE
                    WHEN from_ex.address IS NOT NULL AND to_ex.address IS NULL
                    THEN t.value_eth ELSE 0 END) AS withdrawals
        FROM transactions t
        LEFT JOIN exchange_addresses to_ex
          ON LOWER(t.to_address) = to_ex.address
        LEFT JOIN exchange_addresses from_ex
          ON LOWER(t.from_address) = from_ex.address
        WHERE t.value_eth IS NOT NULL
          AND t.token_value IS NULL
          AND (
              (to_ex.address IS NOT NULL AND from_ex.address IS NULL)
              OR (from_ex.address IS NOT NULL AND to_ex.address IS NULL)
          )
        GROUP BY metric_date, exchange_label
    ),
    token_flows AS (
        SELECT
            date(t.timestamp) AS metric_date,
            CASE
                WHEN to_ex.address IS NOT NULL AND from_ex.address IS NULL THEN to_ex.label
                WHEN from_ex.address IS NOT NULL AND to_ex.address IS NULL THEN from_ex.label
            END AS exchange_label,
            t.token_symbol AS asset_symbol,
            SUM(CASE
                    WHEN to_ex.address IS NOT NULL AND from_ex.address IS NULL
                    THEN t.token_value ELSE 0 END) AS deposits,
            SUM(CASE
                    WHEN from_ex.address IS NOT NULL AND to_ex.address IS NULL
                    THEN t.token_value ELSE 0 END) AS withdrawals
        FROM transactions t
        LEFT JOIN exchange_addresses to_ex
          ON LOWER(t.to_address) = to_ex.address
        LEFT JOIN exchange_addresses from_ex
          ON LOWER(t.from_address) = from_ex.address
        WHERE t.token_value IS NOT NULL
          AND (
              (to_ex.address IS NOT NULL AND from_ex.address IS NULL)
              OR (from_ex.address IS NOT NULL AND to_ex.address IS NULL)
          )
        GROUP BY metric_date, exchange_label, t.token_symbol
    )
    SELECT metric_date, exchange_label, asset_symbol, deposits, withdrawals
    FROM eth_flows
    UNION ALL
    SELECT metric_date, exchange_label, asset_symbol, deposits, withdrawals
    FROM token_flows;
    """
    exchange_df = pd.read_sql(exchange_flow_query, engine)
    if not exchange_df.empty:
        for _, row in exchange_df.iterrows():
            net_flow = row["deposits"] - row["withdrawals"]
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "exchange_deposits",
                    "entity_type": "exchange",
                    "entity_label": row["exchange_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["deposits"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "exchange_withdrawals",
                    "entity_type": "exchange",
                    "entity_label": row["exchange_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": row["withdrawals"],
                }
            )
            metrics.append(
                {
                    "metric_date": row["metric_date"],
                    "metric_name": "exchange_net_flow",
                    "entity_type": "exchange",
                    "entity_label": row["exchange_label"],
                    "asset_symbol": row["asset_symbol"],
                    "value": net_flow,
                }
            )

    return pd.DataFrame(metrics)


def write_daily_metrics(df: pd.DataFrame) -> None:
    if df.empty:
        return
    df.to_sql("daily_metrics", engine, if_exists="append", index=False)


def summarize_flow_metrics(df: pd.DataFrame, allowed_entity_types=None) -> pd.DataFrame:
    if df.empty:
        return df

    flow_metrics = {
        "inflow",
        "outflow",
        "net_flow",
        "tokens_minted",
        "tokens_burned",
        "net_flow_to_exchanges",
        "transfer_count",
        "transfer_count_7d_delta",
        "exchange_deposits",
        "exchange_withdrawals",
        "exchange_net_flow",
    }
    filtered = df[df["metric_name"].isin(flow_metrics)].copy()
    if allowed_entity_types:
        filtered = filtered[
            filtered["entity_type"]
            .astype(str)
            .str.lower()
            .isin([t.lower() for t in allowed_entity_types])
        ]
    if filtered.empty:
        return filtered

    filtered["metric_date"] = pd.to_datetime(filtered["metric_date"], errors="coerce")
    filtered = filtered.dropna(subset=["metric_date", "entity_label", "asset_symbol"])
    filtered = filtered.sort_values("metric_date")
    latest = filtered.groupby(["entity_label", "asset_symbol"]).tail(5)
    pivoted = latest.pivot_table(
        index=["entity_label", "entity_type", "asset_symbol"],
        columns="metric_name",
        values="value",
        aggfunc="last",
    ).reset_index()
    return pivoted
