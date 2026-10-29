-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Governance with Unity Catalog
-- MAGIC
-- MAGIC **What you're learning:** documenting data (comments, tags), access control (GRANT), a
-- MAGIC **column mask** on PII (TIN), a **row filter** (state-level access, a very common federal requirement),
-- MAGIC and **lineage** (open the table in Catalog Explorer → *Lineage* tab).
-- MAGIC
-- MAGIC If you changed the catalog name in config.py, change `fsa` below too.

-- COMMAND ----------

USE CATALOG fsa;

-- COMMAND ----------

-- Document the data. Comments show up in Catalog Explorer and help the AI assistant and Genie.
COMMENT ON TABLE silver.payments IS 'FSA program payments, deduplicated; latest extract wins. Source: FSA-NPS nightly export.';
ALTER TABLE silver.payments ALTER COLUMN payment_amount COMMENT 'Gross payment in USD before offsets';
ALTER TABLE silver.dim_producer SET TAGS ('contains_pii' = 'true', 'data_owner' = 'FSA-OCIO');
ALTER TABLE silver.dim_producer ALTER COLUMN tin SET TAGS ('pii' = 'tin');

-- COMMAND ----------

-- Access control. `account users` exists in every workspace; in a real program these would be
-- groups like `fsa_analysts` and `fsa_engineers` synced from the agency's identity provider.
GRANT USE CATALOG ON CATALOG fsa TO `account users`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `account users`;
SHOW GRANTS ON SCHEMA gold;

-- COMMAND ----------

-- Column mask: only members of fsa_pii_readers see the full TIN.
-- You are NOT in that group, so you will see the masked value. That shows the mask works.
CREATE OR REPLACE FUNCTION ops.mask_tin(tin STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('fsa_pii_readers') THEN tin
            ELSE concat('**-***', right(tin, 4)) END;

ALTER TABLE silver.dim_producer ALTER COLUMN tin SET MASK ops.mask_tin;

SELECT producer_id, producer_name, tin FROM silver.dim_producer LIMIT 5;

-- COMMAND ----------

-- Row filter: users only see payments for their state, unless they are national admins.
CREATE OR REPLACE FUNCTION ops.state_filter(state_code STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('fsa_national_admins') OR state_code IN ('VA', 'IA');

ALTER TABLE silver.payments SET ROW FILTER ops.state_filter ON (state_code);

SELECT state_code, count(*) FROM silver.payments GROUP BY ALL;   -- only VA and IA come back

-- COMMAND ----------

-- Remove the row filter so the rest of the pipeline sees every state
-- (gold is rebuilt from silver, so a filter left on would shrink gold).
ALTER TABLE silver.payments DROP ROW FILTER;

-- COMMAND ----------

-- Lineage is also queryable, if system tables are available in your workspace:
-- SELECT * FROM system.access.table_lineage WHERE target_table_full_name LIKE 'fsa.gold.%' LIMIT 50;
