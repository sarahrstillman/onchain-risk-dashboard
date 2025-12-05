import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv("src/config/.env")

API_KEY = os.getenv("ETHERSCAN_API_KEY")

import os
import requests
import pandas as pd

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")


def fetch_wallet_txs(address: str) -> pd.DataFrame:
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
        "offset": 10000,               # up to 10k txs per page
        "sort": "asc",                 # oldest → newest
    }

    resp = requests.get(url, params=params, timeout=15)
    data = resp.json()

    print("DEBUG Etherscan response:", data)

    if data.get("status") != "1":
        raise RuntimeError(
            f"Etherscan error: {data.get('message')} | result={data.get('result')}"
        )

    # result is a list of tx dicts → turn into DataFrame
    return pd.DataFrame(data["result"])
