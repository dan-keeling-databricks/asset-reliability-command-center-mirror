-- Least-privilege Unity Catalog grants for the Fleet Telemetry Analyst.
--
-- This SQL is intentionally separate from the pipeline graph. Run it only after the
-- pipeline has published `pump_telemetry_history`. Re-running it is safe because GRANT is
-- idempotent. Databricks SQL does not accept parameter markers or IDENTIFIER() in a GRANT
-- principal clause, so the existing target group is quoted explicitly. The checked deploy entry
-- point verifies the operator is a member before this job runs; the script creates no identities.

GRANT USE CATALOG
ON CATALOG IDENTIFIER(:catalog_name)
TO `asset-reliability-command-center-operator`;

GRANT USE SCHEMA
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name)
TO `asset-reliability-command-center-operator`;

GRANT SELECT
ON VIEW IDENTIFIER(:catalog_name || '.' || :schema_name || '.pump_telemetry_history')
TO `asset-reliability-command-center-operator`;

-- Text-readable verification output for deployment logs and the curated evidence package.
SHOW GRANTS
ON VIEW IDENTIFIER(:catalog_name || '.' || :schema_name || '.pump_telemetry_history');
