-- Unity Catalog privileges required by the code-defined workload identity.
--
-- The principal is the already-existing Databricks App service principal for this target.
-- Databricks SQL does not accept parameter markers or IDENTIFIER() in a GRANT principal clause,
-- so the non-secret application ID is quoted explicitly and verified by the checked deploy
-- entry point before this job runs. This script does not create identities or groups. It runs before
-- the continuous producer, RTM ingestion, and governed pipeline runner are switched to that
-- principal. Every GRANT is idempotent.

GRANT USE CATALOG
ON CATALOG IDENTIFIER(:catalog_name)
TO `bd5178cb-8774-4d77-bc63-b4f491716d0b`;

GRANT USE SCHEMA
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name)
TO `bd5178cb-8774-4d77-bc63-b4f491716d0b`;

GRANT CREATE TABLE
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name)
TO `bd5178cb-8774-4d77-bc63-b4f491716d0b`;

GRANT READ VOLUME
ON VOLUME IDENTIFIER(:catalog_name || '.' || :schema_name || '.' || :volume_name)
TO `bd5178cb-8774-4d77-bc63-b4f491716d0b`;

GRANT WRITE VOLUME
ON VOLUME IDENTIFIER(:catalog_name || '.' || :schema_name || '.' || :volume_name)
TO `bd5178cb-8774-4d77-bc63-b4f491716d0b`;

-- These statements produce text-readable deployment evidence of the effective grants.
SHOW GRANTS
ON CATALOG IDENTIFIER(:catalog_name);

SHOW GRANTS
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name);

SHOW GRANTS
ON VOLUME IDENTIFIER(:catalog_name || '.' || :schema_name || '.' || :volume_name);
