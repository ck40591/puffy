CREATE OR REPLACE TABLE puffy.attribution_first_click AS
WITH paid_events AS (
  SELECT
    client_id,
    event_ts,
    session_id,
    page_url,
    referrer,
    event_name
  FROM puffy.events_enriched
  WHERE is_paid_source = true
),
purchases AS (
  SELECT
    client_id,
    event_ts AS purchase_ts,
    session_id AS purchase_session_id,
    event_data,
    page_url
  FROM puffy.events_enriched
  WHERE event_name = 'purchase'
),
joined AS (
  SELECT
    p.client_id,
    p.purchase_ts,
    p.purchase_session_id,
    pe.page_url AS attrib_page_url,
    pe.referrer AS attrib_referrer,
    pe.event_ts AS attrib_ts,
    ROW_NUMBER() OVER (
      PARTITION BY p.client_id, p.purchase_ts
      ORDER BY pe.event_ts ASC
    ) AS rn
  FROM purchases p
  LEFT JOIN paid_events pe
    ON p.client_id = pe.client_id
   AND pe.event_ts <= p.purchase_ts
   AND pe.event_ts >= p.purchase_ts - INTERVAL 7 DAYS
)
SELECT
  client_id,
  purchase_ts,
  purchase_session_id,
  attrib_page_url,
  attrib_referrer
FROM joined
WHERE rn = 1;
