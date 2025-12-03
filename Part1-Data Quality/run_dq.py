from dq_checks import run_all_checks

source_path = "/mnt/puffy_raw/events/"  # adjust to your location

summary_df, dupes_df = run_all_checks(
    spark,
    source_path=source_path,
    save_table="puffy.quality_event_issues"
)

display(summary_df)
display(dupes_df.limit(50))
