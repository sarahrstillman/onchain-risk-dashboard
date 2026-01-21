import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from analytics.metrics import build_daily_metrics, summarize_flow_metrics, write_daily_metrics
from analytics.risk import build_risk_metrics, write_risk_metrics
from src.etl.entities import list_entities, load_entities
from src.etl.enrich import add_contract_flags
from src.etl.fetch import fetch_token_transfers, fetch_wallet_txs
from src.etl.load import load_transactions, normalize


def ingest_wallet(
    wallet_address: str,
    entity_type: str = "",
    max_transfers: int = 1000,
    skip_stablecoins: bool = False,
    since_days: int = 0,
) -> None:
    entity_type = (entity_type or "").lower()
    if skip_stablecoins and entity_type in {"stablecoin", "contract"}:
        return
    if entity_type in {"stablecoin", "contract"}:
        raw = fetch_token_transfers(
            wallet_address,
            max_count=max_transfers,
            since_days=since_days,
        )
    else:
        raw = fetch_wallet_txs(
            wallet_address,
            max_count=max_transfers,
            since_days=since_days,
        )
    normalized = normalize(raw, wallet_address)
    enriched = add_contract_flags(normalized)
    load_transactions(enriched)


def run(
    wallet_address: str,
    top_n: int,
    entities_csv: str,
    large_tx_threshold: float,
    ingest_entities: bool,
    max_transfers: int,
    skip_stablecoins: bool,
    since_days: int,
    skip_risk: bool,
) -> None:
    load_entities(entities_csv)

    if wallet_address:
        ingest_wallet(
            wallet_address,
            max_transfers=max_transfers,
            skip_stablecoins=skip_stablecoins,
            since_days=since_days,
        )
    elif ingest_entities:
        entities = list_entities()
        workers = int(os.getenv("INGEST_WORKERS", "4"))
        workers = max(1, min(workers, len(entities)))
        if workers == 1:
            for entity in entities:
                ingest_wallet(
                    entity["address"],
                    entity.get("entity_type", ""),
                    max_transfers=max_transfers,
                    skip_stablecoins=skip_stablecoins,
                    since_days=since_days,
                )
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        ingest_wallet,
                        entity["address"],
                        entity.get("entity_type", ""),
                        max_transfers,
                        skip_stablecoins,
                        since_days,
                    ): entity
                    for entity in entities
                }
                for future in as_completed(futures):
                    entity = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"Failed to ingest {entity.get('label') or entity['address']}: {exc}")

    metrics = None
    if not skip_risk:
        metrics = build_risk_metrics()
        write_risk_metrics(metrics)

    daily_metrics = build_daily_metrics(large_tx_threshold=large_tx_threshold)
    write_daily_metrics(daily_metrics)

    if not skip_risk:
        if metrics is None or metrics.empty:
            print("No risk metrics available yet.")
        else:
            top = metrics.sort_values("risk_score", ascending=False).head(top_n)
            columns = [
                "wallet_address",
                "risk_score",
                "tx_count_30d",
                "volume_30d",
                "unique_counterparties_30d",
                "avg_tx_size",
            ]
            print("Hot Wallet Risk Scores")
            print(top[columns].to_string(index=False))

    # Exchange flow output removed to keep results focused.

def main() -> None:
    parser = argparse.ArgumentParser(description="Run on-chain risk pipeline.")
    parser.add_argument("wallet_address", nargs="?", help="Wallet address to ingest.")
    parser.add_argument("--top", type=int, default=10, help="Top N wallets to show.")
    parser.add_argument(
        "--entities",
        default="data/entities.csv",
        help="CSV with entity addresses (address,label,entity_type).",
    )
    parser.add_argument(
        "--ingest-entities",
        action="store_true",
        help="Ingest all addresses from the entities table.",
    )
    parser.add_argument(
        "--large-tx-threshold",
        type=float,
        default=1000.0,
        help="ETH threshold for large transfer metrics.",
    )
    parser.add_argument(
        "--max-transfers",
        type=int,
        default=1000,
        help="Max transfers per address (Alchemy/Etherscan).",
    )
    parser.add_argument(
        "--skip-stablecoins",
        action="store_true",
        help="Skip stablecoin/contract ingestion for faster runs.",
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=0,
        help="Only keep transfers within the last N days.",
    )
    parser.add_argument(
        "--skip-risk",
        action="store_true",
        help="Skip risk scoring and only emit flow metrics.",
    )
    args = parser.parse_args()

    if not args.wallet_address and not args.ingest_entities:
        parser.error("Provide a wallet address or use --ingest-entities.")

    run(
        args.wallet_address,
        args.top,
        args.entities,
        args.large_tx_threshold,
        args.ingest_entities,
        args.max_transfers,
        args.skip_stablecoins,
        args.since_days,
        args.skip_risk,
    )


if __name__ == "__main__":
    main()
