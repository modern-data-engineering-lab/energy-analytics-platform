-- Per-outage-type classifier accuracy on the held-out test set — which outage types the
-- XGBoost classifier actually predicts well versus poorly, not just one blended accuracy
-- number. See README's "Why this repo's classifier reports ~66% accuracy" for why this
-- number is lower than, and more honest than, the thesis's own.
--
-- Replace {database} as in mttr_mtbf_leaderboard.sql.
CREATE OR REPLACE VIEW classifier_accuracy_by_type AS
SELECT
    outage_type_canonical AS actual_outage_type,
    COUNT(*) AS test_set_events,
    SUM(CASE WHEN correct THEN 1 ELSE 0 END) AS correct_predictions,
    ROUND(100.0 * SUM(CASE WHEN correct THEN 1 ELSE 0 END) / COUNT(*), 1) AS accuracy_pct
FROM {database}.classifier_predictions
GROUP BY outage_type_canonical
ORDER BY test_set_events DESC;
