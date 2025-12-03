# Part 4 – Production Monitoring

Monitored:
- Volume: daily raw_events and unique_clients.
- Purchases: daily purchases and purchasing_clients.
- Stability: 7‑day rolling mean and standard deviation with 3‑sigma anomaly flags.

Usage:
- Scheduled job runs run_monitoring.py daily after pipeline.
- Alerts wired to anomalies where events_anomaly or purchases_anomaly is true.
- Extensible to dimensioned monitoring (by device_type, channel) as needed.
