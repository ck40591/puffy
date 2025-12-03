df = spark.table("puffy.events_enriched")
df.display()

# Example: funnel counts
funnel = (
    df.groupBy("event_name")
      .count()
      .orderBy("count", ascending=False)
)
display(funnel)
