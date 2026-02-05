from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))

REASON_COLUMNS = [
    "reason_velocity",
    "reason_new_counterparties",
    "reason_contract_interactions",
]


def _format_eth(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.4f}"


def _format_int(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{int(value):,}"


def _format_date(value: Optional[str]) -> str:
    if not value or pd.isna(value):
        return "n/a"
    return str(value)


def _top_reasons(row: pd.Series) -> list[str]:
    scored = {}
    for col in REASON_COLUMNS:
        if col in row:
            scored[col.replace("reason_", "")] = row[col]
    ranked = [
        name for name, score in sorted(scored.items(), key=lambda item: item[1], reverse=True)
        if score and score > 0
    ]
    return ranked


def generate_case_report(wallet_address: str, output_path: Optional[str] = None) -> str:
    wallet = wallet_address.lower().strip()

    risk_query = """
        SELECT *
        FROM risk_metrics
        WHERE LOWER(wallet_address) = :wallet
        ORDER BY as_of_date DESC
        LIMIT 1;
    """
    risk_df = pd.read_sql(risk_query, engine, params={"wallet": wallet})
    risk_row = risk_df.iloc[0] if not risk_df.empty else None

    counterparties_query = """
        SELECT
          CASE WHEN direction = 'out' THEN to_address ELSE from_address END AS counterparty,
          COUNT(*) AS tx_count,
          SUM(value_eth) AS volume_eth
        FROM transactions
        WHERE LOWER(wallet_address) = :wallet
          AND timestamp >= datetime('now', '-30 days')
        GROUP BY counterparty
        ORDER BY volume_eth DESC
        LIMIT 5;
    """
    counterparties_df = pd.read_sql(counterparties_query, engine, params={"wallet": wallet})

    largest_txs_query = """
        SELECT
          timestamp,
          direction,
          from_address,
          to_address,
          value_eth,
          tx_hash
        FROM transactions
        WHERE LOWER(wallet_address) = :wallet
          AND timestamp >= datetime('now', '-30 days')
        ORDER BY value_eth DESC
        LIMIT 10;
    """
    largest_txs_df = pd.read_sql(largest_txs_query, engine, params={"wallet": wallet})

    contract_interactions_query = """
        SELECT
          to_address AS contract_address,
          COUNT(*) AS tx_count,
          SUM(value_eth) AS volume_eth
        FROM transactions
        WHERE LOWER(wallet_address) = :wallet
          AND timestamp >= datetime('now', '-30 days')
          AND is_contract_interaction = 1
        GROUP BY contract_address
        ORDER BY tx_count DESC
        LIMIT 5;
    """
    contract_df = pd.read_sql(contract_interactions_query, engine, params={"wallet": wallet})

    risk_events_query = """
        SELECT rule_name, severity, event_time, details
        FROM risk_events
        WHERE LOWER(wallet_address) = :wallet
        ORDER BY event_time DESC
        LIMIT 10;
    """
    risk_events_df = pd.read_sql(risk_events_query, engine, params={"wallet": wallet})

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"# Case Report: {wallet_address}",
        "",
        f"Generated: {generated_at}",
        "",
        "## Risk Summary",
    ]

    if risk_row is None:
        lines.append("- No risk metrics available for this wallet yet.")
    else:
        top_reasons = _top_reasons(risk_row)
        lines.extend([
            f"- As of: {_format_date(risk_row.get('as_of_date'))}",
            f"- Risk score: {risk_row.get('risk_score', 'n/a')}",
            f"- 30d tx count: {_format_int(risk_row.get('tx_count_30d'))}",
            f"- 30d volume (ETH): {_format_eth(risk_row.get('volume_30d'))}",
            f"- 30d unique counterparties: {_format_int(risk_row.get('unique_counterparties_30d'))}",
            f"- 30d contract interactions: {_format_int(risk_row.get('contract_interactions_30d'))}",
            f"- Avg tx size (ETH): {_format_eth(risk_row.get('avg_tx_size'))}",
            f"- Top reasons: {', '.join(top_reasons) if top_reasons else 'none'}",
        ])

    lines.extend(["", "## Evidence", "", "### Top Counterparties (30d)"])
    if counterparties_df.empty:
        lines.append("- None found.")
    else:
        lines.append("| Counterparty | Tx Count | Volume (ETH) |")
        lines.append("| --- | --- | --- |")
        for _, row in counterparties_df.iterrows():
            lines.append(
                f"| {row['counterparty']} | {_format_int(row['tx_count'])} | {_format_eth(row['volume_eth'])} |"
            )

    lines.extend(["", "### Largest Transfers (30d)"])
    if largest_txs_df.empty:
        lines.append("- None found.")
    else:
        lines.append("| Timestamp | Direction | Counterparty | Value (ETH) | Tx Hash |")
        lines.append("| --- | --- | --- | --- | --- |")
        for _, row in largest_txs_df.iterrows():
            counterparty = row["to_address"] if row["direction"] == "out" else row["from_address"]
            lines.append(
                "| {timestamp} | {direction} | {counterparty} | {value} | {tx_hash} |".format(
                    timestamp=row["timestamp"],
                    direction=row["direction"],
                    counterparty=counterparty,
                    value=_format_eth(row["value_eth"]),
                    tx_hash=row["tx_hash"],
                )
            )

    lines.extend(["", "### Contract Interactions (30d)"])
    if contract_df.empty:
        lines.append("- None found.")
    else:
        lines.append("| Contract | Tx Count | Volume (ETH) |")
        lines.append("| --- | --- | --- |")
        for _, row in contract_df.iterrows():
            lines.append(
                f"| {row['contract_address']} | {_format_int(row['tx_count'])} | {_format_eth(row['volume_eth'])} |"
            )

    lines.extend(["", "### Risk Events (latest)"])
    if risk_events_df.empty:
        lines.append("- None found.")
    else:
        lines.append("| Rule | Severity | Event Time | Details |")
        lines.append("| --- | --- | --- | --- |")
        for _, row in risk_events_df.iterrows():
            details = row["details"] if row["details"] else ""
            lines.append(
                f"| {row['rule_name']} | {_format_int(row['severity'])} | {row['event_time']} | {details} |"
            )

    report = "\n".join(lines) + "\n"

    if not output_path:
        safe_wallet = wallet_address[:10].lower()
        output_path = os.path.join("reports", f"case_{safe_wallet}_{datetime.utcnow().date().isoformat()}.md")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(report)

    return output_path
