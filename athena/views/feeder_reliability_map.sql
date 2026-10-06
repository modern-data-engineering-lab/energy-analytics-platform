-- Reliability metrics per feeder with each feeder's geocoded location, ready to plot.
--
-- LEFT JOIN from the metrics side so every feeder keeps its MTTR/MTBF row even when it couldn't
-- be located. Unlocated feeders come through with a NULL location rather than disappearing.
-- `confidence` is carried through so a map or downstream analysis can filter to
-- high/medium-confidence locations instead of treating every point as equally trustworthy.
--
-- Run once per environment: replace {database} with the Glue database name (see Terraform
-- output `glue_database`, e.g. "energy_analytics_stg").
CREATE OR REPLACE VIEW feeder_reliability_map AS
SELECT
    m.feeder_canonical,
    m.total_outages,
    m.mttr_hours,
    m.mtbf_hours,
    m.total_customer_hours_interruption,
    l.confidence AS location_confidence,
    l.setting,
    TRY_CAST(l.latitude AS DOUBLE) AS latitude,
    TRY_CAST(l.longitude AS DOUBLE) AS longitude,
    TRY_CAST(l.service_radius_km AS DOUBLE) AS service_radius_km,
    -- NULL for unlocated feeders: TRY_CAST('') is NULL, and so is ST_Point of a NULL.
    ST_AsText(ST_Point(TRY_CAST(l.longitude AS DOUBLE), TRY_CAST(l.latitude AS DOUBLE)))
        AS location_wkt
FROM {database}.mttr_mtbf_by_feeder m
LEFT JOIN {database}.feeder_locations l
    ON m.feeder_canonical = l.feeder_canonical
ORDER BY m.mtbf_hours ASC;
