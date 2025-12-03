# Part 1 – Data Quality Framework

Checks:
- Schema: required columns present, no unexpected columns.
- Completeness: null/empty counts on client_id, page_url, timestamp, event_name.
- Timestamp format: parsable to a valid timestamp.
- Valid event names: only allowed marketing events.
- Duplicate detection: identical (client_id, timestamp, event_name, page_url).
- JSON shape: event_data must be valid JSON when populated.

These checks catch: missing/extra columns, ingestion schema drift, malformed timestamps, invalid events, duplicate ingestion, and corrupt JSON – all of which can cause revenue misreporting.

Issues you observe on the 14‑day dataset should be summarized here after running the checks.
