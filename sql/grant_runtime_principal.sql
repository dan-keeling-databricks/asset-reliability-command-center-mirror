-- Unity Catalog privileges required by the code-defined workload identity.
--
-- The principal is the already-existing Databricks App service principal supplied by the
-- bundle. This script does not create identities or groups. It is run by the deployer before
-- the continuous producer, RTM ingestion, and governed pipeline runner are switched to that
-- principal. Every GRANT is idempotent.

GRANT USE CATALOG
ON CATALOG IDENTIFIER(:catalog_name)
TO IDENTIFIER(:principal_name);

GRANT USE SCHEMA, CREATE TABLE
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name)
TO IDENTIFIER(:principal_name);

GRANT READ VOLUME, WRITE VOLUME
ON VOLUME IDENTIFIER(:catalog_name || '.' || :schema_name || '.' || :volume_name)
TO IDENTIFIER(:principal_name);

-- These statements produce text-readable deployment evidence of the effective grants.
SHOW GRANTS
ON CATALOG IDENTIFIER(:catalog_name);

SHOW GRANTS
ON SCHEMA IDENTIFIER(:catalog_name || '.' || :schema_name);

SHOW GRANTS
ON VOLUME IDENTIFIER(:catalog_name || '.' || :schema_name || '.' || :volume_name);
