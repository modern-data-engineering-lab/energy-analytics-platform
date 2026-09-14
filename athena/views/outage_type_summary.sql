-- Outage-type distribution across all feeders — the SQL equivalent of the thesis's Fig. 10
-- bar chart, kept queryable instead of a static image.
--
-- Replace {database} as in mttr_mtbf_leaderboard.sql.
CREATE OR REPLACE VIEW outage_type_summary AS
SELECT
    outage_type_canonical,
    COUNT(*) AS total_events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_all_events,
    ROUND(AVG(duration_hours_num), 2) AS avg_duration_hours,
    ROUND(SUM(customer_hours_interruption_num), 1) AS total_customer_hours_interruption
FROM {database}.classifier_features
WHERE NOT is_transformer_event
GROUP BY outage_type_canonical
ORDER BY total_events DESC;
