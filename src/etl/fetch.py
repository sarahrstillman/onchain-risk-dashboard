import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv("src/config/.env")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ALCHEMY_URL = os.getenv("ALCHEMY_URL")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _alchemy_request(params):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [params],
    }
    resp = requests.post(ALCHEMY_URL, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Alchemy error: {data['error']}")
    return data.get("result", {})


def _alchemy_transfers(
    address: str,
    direction_key: str,
    categories,
    contract_addresses=None,
    max_count: int = 1000,
) -> pd.DataFrame:
    if not ALCHEMY_URL:
        raise RuntimeError("ALCHEMY_URL is not set in your environment.")

    params = {
        "fromBlock": "0x0",
        "toBlock": "latest",
        "category": categories,
        "withMetadata": True,
        "excludeZeroValue": False,
        "maxCount": hex(max_count),
    }
    if direction_key:
        params[direction_key] = address
    if contract_addresses:
        params["contractAddresses"] = contract_addresses

    result = _alchemy_request(params)
    transfers = result.get("transfers", [])

    rows = []
    for item in transfers:
        category = item.get("category")
        value = item.get("value")
        value_wei = None
        token_value = None
        token_symbol = None
        token_contract_address = None

        if category == "erc20":
            raw = item.get("rawContract", {}) or {}
            token_contract_address = raw.get("address")
            decimals = raw.get("decimals") or raw.get("decimal")
            raw_value = raw.get("value")
            token_symbol = item.get("asset")
            if raw_value and decimals is not None:
                decimals_int = int(decimals, 16) if isinstance(decimals, str) and decimals.startswith("0x") else int(decimals)
                token_value = int(raw_value, 16) / (10 ** decimals_int)
            elif value is not None:
                token_value = value
        else:
            if value is not None:
                value_wei = str(int(Decimal(str(value)) * Decimal("1e18")))
        rows.append(
            {
                "hash": item.get("hash"),
                "from": item.get("from"),
                "to": item.get("to"),
                "value": value_wei,
                "blockNumber": item.get("blockNum"),
                "timeStamp": item.get("metadata", {}).get("blockTimestamp"),
                "category": category,
                "token_symbol": token_symbol,
                "token_value": token_value,
                "token_contract_address": token_contract_address,
            }
        )

    return pd.DataFrame(rows)


def _filter_since_days(df: pd.DataFrame, since_days: int) -> pd.DataFrame:
    if df.empty or not since_days or since_days <= 0:
        return df

    raw = df.get("timeStamp")
    if raw is None:
        return df

    numeric = pd.to_numeric(raw, errors="coerce")
    ts_numeric = pd.to_datetime(numeric, unit="s", errors="coerce", utc=True)
    ts_iso = pd.to_datetime(raw, errors="coerce", utc=True)
    ts = ts_numeric.where(ts_numeric.notna(), ts_iso)

    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    return df[ts >= cutoff].copy()


def _fetch_wallet_txs_alchemy(
    address: str,
    max_count: int = 1000,
    since_days: int = 0,
) -> pd.DataFrame:
    categories = ["external", "internal"]
    outbound = _alchemy_transfers(address, "fromAddress", categories, max_count=max_count)
    inbound = _alchemy_transfers(address, "toAddress", categories, max_count=max_count)
    if outbound.empty and inbound.empty:
        return pd.DataFrame([])
    df = pd.concat([outbound, inbound], ignore_index=True)
    df = df.drop_duplicates(subset=["hash", "from", "to", "value", "blockNumber"])
    return _filter_since_days(df, since_days)


def _fetch_token_transfers_alchemy(
    contract_address: str,
    max_count: int = 1000,
    since_days: int = 0,
) -> pd.DataFrame:
    categories = ["erc20"]
    df = _alchemy_transfers(
        contract_address,
        direction_key=None,
        categories=categories,
        contract_addresses=[contract_address],
        max_count=max_count,
    )
    if df.empty:
        return df
    return _filter_since_days(df, since_days)

def fetch_wallet_txs(address: str, max_count: int = 1000, since_days: int = 0) -> pd.DataFrame:
    if ALCHEMY_URL:
        try:
            return _fetch_wallet_txs_alchemy(address, max_count=max_count, since_days=since_days)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is None or status < 500 or not ETHERSCAN_API_KEY:
                raise
        except requests.RequestException:
            if not ETHERSCAN_API_KEY:
                raise

    if not ETHERSCAN_API_KEY:
        raise RuntimeError("ETHERSCAN_API_KEY is not set in your environment.")

    url = "https://api.etherscan.io/v2/api"

    params = {
        "apikey": ETHERSCAN_API_KEY,   # your key
        "chainid": "1",                # Ethereum mainnet
        "module": "account",           # from docs: default 'account'
        "action": "txlist",            # from docs: default 'txlist'
        "address": address,            # wallet we’re querying
        "startblock": 0,
        "endblock": 9999999999,
        "page": 1,
    # docs default offset=1, but we can ask for more per page:
    "offset": max_count,           # up to 1k txs per page
    "sort": "desc",                # newest → oldest
    }

    resp = requests.get(url, params=params, timeout=15)
    data = resp.json()

    if os.getenv("DEBUG_ETHERSCAN") == "1":
        print("DEBUG Etherscan response:", data)

    if data.get("status") != "1":
        if data.get("message") == "No transactions found":
            return pd.DataFrame([])
        raise RuntimeError(
            f"Etherscan error: {data.get('message')} | result={data.get('result')}"
        )

    # result is a list of tx dicts → turn into DataFrame
    return _filter_since_days(pd.DataFrame(data["result"]), since_days)


def fetch_token_transfers(
    contract_address: str,
    max_count: int = 1000,
    since_days: int = 0,
) -> pd.DataFrame:
    if not ALCHEMY_URL:
        raise RuntimeError("ALCHEMY_URL is required for token transfer ingestion.")
    return _fetch_token_transfers_alchemy(
        contract_address,
        max_count=max_count,
        since_days=since_days,
    )
