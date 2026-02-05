ALTER TABLE risk_metrics ADD COLUMN contract_interactions_30d INTEGER;
ALTER TABLE risk_metrics ADD COLUMN reason_velocity REAL;
ALTER TABLE risk_metrics ADD COLUMN reason_new_counterparties REAL;
ALTER TABLE risk_metrics ADD COLUMN reason_contract_interactions REAL;

CREATE TABLE IF NOT EXISTS audit_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT,
    as_of_date TEXT,
    risk_score REAL,
    top_reasons TEXT,
    pipeline_version TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
