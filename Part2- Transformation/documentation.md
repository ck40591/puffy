# Part 2 – Transformation

- Sessionization: 30‑minute inactivity window per client_id; sessions built with window over sorted events.
- Devices: derived from user_agent into mobile vs desktop.
- Paid/owned: detected from presence of utm_* in referrer or page_url.
- Attribution: first‑click and last‑click defined over 7‑day lookback per client and purchase.

Key tables:
- puffy.events_enriched: one row per raw event with parsed_ts, device_type, is_paid_source, session_id.
- puffy.sessions: one row per session with start/end and primary device.
- puffy.attribution_first_click / last_click: one row per purchase with attributed page + referrer.
