from pyspark.sql import functions as F, Window

VALIDATED_TABLE = "puffy.raw_events_validated"
SESSIONS_TABLE = "puffy.sessions"
EVENTS_ENRICHED_TABLE = "puffy.events_enriched"

SESSION_TIMEOUT_MIN = 30

def parse_base_events(spark):
    df = spark.table("puffy.raw_events")  # or ingest from bronze and filter by DQ

    df = df.withColumn("event_ts", F.to_timestamp("timestamp"))

    df = df.withColumn(
        "device_type",
        F.when(F.lower("user_agent").like("%mobile%"), F.lit("mobile"))
         .when(F.lower("user_agent").like("%iphone%"), F.lit("mobile"))
         .when(F.lower("user_agent").like("%android%"), F.lit("mobile"))
         .otherwise(F.lit("desktop"))
    )

    df = df.withColumn(
        "is_paid_source",
        F.when(F.col("referrer").rlike("utm_"), F.lit(True))
         .when(F.col("page_url").rlike("utm_"), F.lit(True))
         .otherwise(F.lit(False))
    )

    return df

def create_sessions(df):
    w = Window.partitionBy("client_id").orderBy("event_ts")

    df = df.withColumn(
        "prev_ts",
        F.lag("event_ts").over(w)
    ).withColumn(
        "minutes_since_prev",
        F.round(F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60
    ).withColumn(
        "new_session_flag",
        F.when(
            (F.col("prev_ts").isNull()) |
            (F.col("minutes_since_prev") > SESSION_TIMEOUT_MIN),
            1
        ).otherwise(0)
    ).withColumn(
        "session_id",
        F.concat_ws(
            "-",
            F.col("client_id"),
            F.sum("new_session_flag").over(w.rowsBetween(Window.unboundedPreceding, 0))
        )
    )

    sessions = (
        df.groupBy("session_id", "client_id")
          .agg(
              F.min("event_ts").alias("session_start_ts"),
              F.max("event_ts").alias("session_end_ts"),
              F.first("device_type").alias("session_device_type"),
          )
    )

    return df, sessions

def write_transformed(spark):
    base_df = parse_base_events(spark)
    events_with_session, sessions = create_sessions(base_df)

    (events_with_session
         .write.mode("overwrite")
         .saveAsTable(EVENTS_ENRICHED_TABLE))

    (sessions
         .write.mode("overwrite")
         .saveAsTable(SESSIONS_TABLE))

    return events_with_session, sessions
