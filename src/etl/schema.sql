CREATE TABLE IF NOT EXISTS wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE,
    label TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_hash TEXT,
    wallet_address TEXT,
    direction TEXT,
    from_address TEXT,
    to_address TEXT,
    value_eth REAL,
    block_number INTEGER,
    timestamp TEXT,
    token_symbol TEXT,
    token_value REAL,
    is_contract_interaction BOOLEAN
);

CREATE TABLE IF NOT EXISTS risk_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT,
    as_of_date TEXT,
    tx_count_30d INTEGER,
    volume_30d REAL,
    unique_counterparties_30d INTEGER,
    avg_tx_size REAL,
    risk_score REAL
);

CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT,
    rule_name TEXT,
    severity INTEGER,
    event_time TEXT DEFAULT CURRENT_TIMESTAMP,
    details TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE,
    label TEXT,
    entity_type TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_date TEXT,
    metric_name TEXT,
    entity_type TEXT,
    entity_label TEXT,
    asset_symbol TEXT,
    value REAL
);
