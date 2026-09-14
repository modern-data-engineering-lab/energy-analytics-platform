-- Which feeders most need maintenance attention, ranked by reliability.
--
-- Low MTBF (mean time between failure) = fails often. High MTTR (mean time to restore) = slow
-- to fix once it does. A feeder that's bad on both is the clearest maintenance priority; sorted
-- by MTBF ascending (least reliable first) since "fails often" is generally the more urgent
-- signal than "slow to fix" on its own.
--
-- Run once per environment: replace {database} with the Glue database name (see Terraform
-- output `glue_database`, e.g. "energy_analytics_stg").
CREATE OR REPLACE VIEW mttr_mtbf_leaderboard AS
SELECT
    feeder_canonical,
    total_outages,
    mttr_hours,
    mtbf_hours,
    total_customer_hours_interruption,
    RANK() OVER (ORDER BY mtbf_hours ASC) AS reliability_rank
FROM {database}.mttr_mtbf_by_feeder
ORDER BY mtbf_hours ASC;
