from pyspark.sql import functions as F, types as T

RAW_TABLE = "puffy.raw_events"
EVENTS_TABLE = "puffy.events_enriched"
ATTR_LAST = "puffy.attribution_last_click"

MONITOR_TABLE = "puffy.monitor_daily_metrics"

def daily_ingestion_metrics(spark):
    df = spark.table(RAW_TABLE).withColumn("event_date", F.to_date("timestamp"))
    return (
        df.groupBy("event_date")
          .agg(
              F.count("*").alias("raw_events"),
              F.countDistinct("client_id").alias("unique_clients")
          )
    )

def daily_purchase_metrics(spark):
    df = spark.table(EVENTS_TABLE).withColumn("event_date", F.to_date("event_ts"))
    purchases = df.filter(F.col("event_name") == "purchase")
    return (
        purchases.groupBy("event_date")
                 .agg(
                     F.count("*").alias("purchases"),
                     F.countDistinct("client_id").alias("purchasing_clients")
                 )
    )

def join_metrics(spark):
    ing = daily_ingestion_metrics(spark)
    pur = daily_purchase_metrics(spark)
    return (
        ing.join(pur, "event_date", "left")
           .fillna({"purchases": 0, "purchasing_clients": 0})
    )

def detect_anomalies(metrics_df):
    w = Window.orderBy("event_date").rowsBetween(-7, -1)

    metrics_df = metrics_df.withColumn(
        "events_avg_7d",
        F.avg("raw_events").over(w)
    ).withColumn(
        "events_std_7d",
        F.stddev("raw_events").over(w)
    ).withColumn(
        "purchases_avg_7d",
        F.avg("purchases").over(w)
    ).withColumn(
        "purchases_std_7d",
        F.stddev("purchases").over(w)
    ).withColumn(
        "events_anomaly",
        F.when(
            (F.col("events_avg_7d").isNotNull()) &
            (F.col("events_std_7d").isNotNull()) &
            (F.abs(F.col("raw_events") - F.col("events_avg_7d")) > 3 * F.col("events_std_7d")),
            True
        ).otherwise(False)
    ).withColumn(
        "purchases_anomaly",
        F.when(
            (F.col("purchases_avg_7d").isNotNull()) &
            (F.col("purchases_std_7d").isNotNull()) &
            (F.abs(F.col("purchases") - F.col("purchases_avg_7d")) > 3 * F.col("purchases_std_7d")),
            True
        ).otherwise(False)
    )

    return metrics_df

def run_monitoring(spark):
    metrics = join_metrics(spark)
    metrics_with_flags = detect_anomalies(metrics)

    (metrics_with_flags
         .write.mode("overwrite")
         .saveAsTable(MONITOR_TABLE))

    anomalies = metrics_with_flags.filter("events_anomaly OR purchases_anomaly")
    return metrics_with_flags, anomalies
