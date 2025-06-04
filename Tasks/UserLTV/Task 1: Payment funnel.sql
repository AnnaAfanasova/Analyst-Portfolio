
-- 1.1 Payment funnel
-- Завдання: Написати SQL запит, який відтворюватиме воронку користувача в рамках кожної підписки з урахуванням рефандів. 
WITH events AS (
  SELECT uuid, product_id, transaction_id, event_timestamp, revenue_usd,
    FIRST_VALUE(transaction_id) OVER (PARTITION BY uuid, product_id ORDER BY event_timestamp) AS original_transaction_id,
    CASE 
      WHEN event_name = 'purchase' 
      THEN ROW_NUMBER() OVER (PARTITION BY uuid, product_id, event_name ORDER BY event_timestamp)
      ELSE NULL
    END AS renewal_number
  FROM subscription_events
  )

SELECT uuid, product_id, transaction_id, original_transaction_id, revenue_usd, renewal_number
FROM events
ORDER BY uuid, event_timestamp;


-- 1.2 User purchases
-- Завдання: Написати SQL запит, результат якого міститиме агреговану інформацію про покупки кожного користувача
WITH subscription_bounds AS (
  SELECT
    uuid,
    product_id,
    MAX(event_timestamp) FILTER (WHERE event_name IN ('trial','purchase'))
      + MAX(period) FILTER (WHERE event_name IN ('trial','purchase')) * INTERVAL '1 day'
      AS expiration_time,
    MAX(event_timestamp) FILTER (WHERE event_name = 'cancellation') AS cancellation_time
  FROM subscription_events
  GROUP BY uuid, product_id
),
current_sub AS (
  -- For test data: pick the latest expiration_time per user;
  -- for live data, add a WHERE expiration_time > NOW() to filter only active subscriptions
  SELECT DISTINCT ON (uuid) 
    uuid, product_id AS current_product_id, expiration_time, cancellation_time
  FROM subscription_bounds
  ORDER BY uuid, expiration_time DESC
),
metrics AS (
  SELECT
    uuid,
    MIN(event_timestamp)   FILTER (WHERE event_name = 'trial')    AS trial_started_time,
    MIN(event_timestamp)   FILTER (WHERE event_name = 'purchase') AS first_purchase_time,
    MAX(event_timestamp)   FILTER (WHERE event_name = 'purchase') AS last_purchase_time,
    COUNT(*)               FILTER (WHERE event_name = 'purchase') AS total_purchases,
    SUM(revenue_usd)       FILTER (WHERE event_name = 'purchase' OR event_name = 'refund') AS total_revenue_usd,
    MAX(event_timestamp)   FILTER (WHERE event_name = 'refund')   AS refund_time
  FROM subscription_events
  GROUP BY uuid
)
SELECT m.uuid, cs.current_product_id, m.trial_started_time, m.first_purchase_time, m.last_purchase_time, 
       m.total_purchases, m.total_revenue_usd, cs.expiration_time, cs.cancellation_time, m.refund_time
FROM metrics m
LEFT JOIN current_sub cs ON m.uuid = cs.uuid
ORDER BY m.uuid;


