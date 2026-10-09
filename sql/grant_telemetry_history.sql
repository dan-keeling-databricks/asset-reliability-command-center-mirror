-- Least-privilege Unity Catalog grants for the Fleet Telemetry Analyst.
--
-- This SQL is intentionally separate from the pipeline graph. Run it only after the
-- pipeline has published `pump_telemetry_history`. Re-running it is safe because GRANT is
-- idempotent. The principal is the existing workspace group supplied by the bundle; this
-- script never creates or manages identities.

GRANT USE CATALOG
ON CATALOG IDENTIFIER(:catalog_name)
TO IDENTIFIER(:principal_name);

GRANT USE SCHEMA
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name)
TO IDENTIFIER(:principal_name);

GRANT SELECT
ON VIEW IDENTIFIER(:catalog_name || '.' || :schema_name || '.pump_telemetry_history')
TO IDENTIFIER(:principal_name);

-- Text-readable verification output for deployment logs and the curated evidence package.
SHOW GRANTS
ON VIEW IDENTIFIER(:catalog_name || '.' || :schema_name || '.pump_telemetry_history');
