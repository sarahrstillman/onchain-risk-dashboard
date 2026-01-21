SELECT
  wallet_address,
  COUNT(*) AS tx_count_30d,
  SUM(value_eth) AS volume_30d,
  COUNT(DISTINCT CASE WHEN direction = 'out' THEN to_address ELSE from_address END)
      AS unique_counterparties_30d,
  AVG(value_eth) AS avg_tx_size
FROM transactions
WHERE timestamp >= datetime('now', '-30 days')
GROUP BY wallet_address;

-- Daily net flows by entity (requires entities table)
SELECT
  date(t.timestamp) AS metric_date,
  e.entity_type,
  e.label AS entity_label,
  SUM(CASE WHEN LOWER(t.to_address) = e.address THEN t.value_eth ELSE 0 END) AS inflow,
  SUM(CASE WHEN LOWER(t.from_address) = e.address THEN t.value_eth ELSE 0 END) AS outflow,
  SUM(CASE WHEN LOWER(t.to_address) = e.address THEN t.value_eth ELSE 0 END)
    - SUM(CASE WHEN LOWER(t.from_address) = e.address THEN t.value_eth ELSE 0 END)
    AS net_flow
FROM transactions t
JOIN entities e
  ON LOWER(t.to_address) = e.address OR LOWER(t.from_address) = e.address
GROUP BY metric_date, e.entity_type, e.label;

-- Large transfer counts by day
SELECT
  date(timestamp) AS metric_date,
  COUNT(*) AS large_tx_count,
  SUM(value_eth) AS large_tx_volume
FROM transactions
WHERE value_eth >= 1000
GROUP BY metric_date;

-- Stablecoin flow metrics (requires token_value + entities table)
SELECT
  date(t.timestamp) AS metric_date,
  e.entity_type,
  e.label AS entity_label,
  t.token_symbol AS asset_symbol,
  SUM(CASE WHEN LOWER(t.from_address) = '0x0000000000000000000000000000000000000000' THEN t.token_value ELSE 0 END) AS tokens_minted,
  SUM(CASE WHEN LOWER(t.to_address) = '0x0000000000000000000000000000000000000000' THEN t.token_value ELSE 0 END) AS tokens_burned,
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
WHERE LOWER(e.entity_type) IN ('stablecoin', 'contract')
  AND t.token_value IS NOT NULL
GROUP BY metric_date, e.entity_type, e.label, t.token_symbol;

-- Exchange net flow (deposits/withdrawals; excludes exchange↔exchange)
WITH exchange_addresses AS (
  SELECT address
  FROM entities
  WHERE LOWER(entity_type) LIKE '%exchange%' OR LOWER(entity_type) LIKE '%hot%'
),
eth_flows AS (
  SELECT
    date(t.timestamp) AS metric_date,
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
  GROUP BY metric_date
),
token_flows AS (
  SELECT
    date(t.timestamp) AS metric_date,
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
  GROUP BY metric_date, t.token_symbol
)
SELECT
  metric_date,
  asset_symbol,
  deposits,
  withdrawals,
  deposits - withdrawals AS net_flow
FROM eth_flows
UNION ALL
SELECT
  metric_date,
  asset_symbol,
  deposits,
  withdrawals,
  deposits - withdrawals AS net_flow
FROM token_flows;
