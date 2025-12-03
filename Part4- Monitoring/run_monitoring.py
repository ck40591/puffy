from monitoring_checks import run_monitoring

metrics, anomalies = run_monitoring(spark)
display(metrics.orderBy("event_date"))
display(anomalies)
