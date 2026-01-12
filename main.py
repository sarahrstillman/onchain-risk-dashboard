import argparse

from analytics.metrics import build_daily_metrics, write_daily_metrics
from analytics.risk import build_risk_metrics, write_risk_metrics
from src.etl.entities import load_entities
from src.etl.enrich import add_contract_flags
from src.etl.fetch import fetch_wallet_txs
from src.etl.load import load_transactions, normalize


def run(wallet_address: str, top_n: int, entities_csv: str, large_tx_threshold: float) -> None:
    load_entities(entities_csv)

    raw = fetch_wallet_txs(wallet_address)
    normalized = normalize(raw, wallet_address)
    enriched = add_contract_flags(normalized)
    load_transactions(enriched)

    metrics = build_risk_metrics()
    write_risk_metrics(metrics)

    daily_metrics = build_daily_metrics(large_tx_threshold=large_tx_threshold)
    write_daily_metrics(daily_metrics)

    if metrics.empty:
        print("No risk metrics available yet.")
        return

    top = metrics.sort_values("risk_score", ascending=False).head(top_n)
    columns = [
        "wallet_address",
        "risk_score",
        "tx_count_30d",
        "volume_30d",
        "unique_counterparties_30d",
        "avg_tx_size",
    ]
    print(top[columns].to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run on-chain risk pipeline.")
    parser.add_argument("wallet_address", help="Wallet address to ingest.")
    parser.add_argument("--top", type=int, default=10, help="Top N wallets to show.")
    parser.add_argument(
        "--entities",
        default="data/entities.csv",
        help="CSV with entity addresses (address,label,entity_type).",
    )
    parser.add_argument(
        "--large-tx-threshold",
        type=float,
        default=1000.0,
        help="ETH threshold for large transfer metrics.",
    )
    args = parser.parse_args()

    run(args.wallet_address, args.top, args.entities, args.large_tx_threshold)


if __name__ == "__main__":
    main()
