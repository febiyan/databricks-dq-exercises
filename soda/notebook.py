# Databricks notebook source
# DBTITLE 1,Install Soda Core
# MAGIC %sh
# MAGIC
# MAGIC pip install soda-core-spark-df==3.5.5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Validation Checks

# COMMAND ----------

# DBTITLE 1,Variables
scan_name = "Bakehouse Transactions Checks"
data_source_path = "samples.bakehouse.sales_transactions"
quarantine_table_path = "workspace.default.sales_transactions_checks"

# COMMAND ----------

# DBTITLE 1,Setup Scan Objects
# import Scan from Soda Library
from soda.scan import Scan
from custom_sampler import UnityCatalogSampler

scan = Scan()
scan.sampler = UnityCatalogSampler(spark=spark, failed_table_path=quarantine_table_path)
scan.set_scan_definition_name(scan_name)
scan.set_data_source_name(data_source_path)
scan.add_spark_session(spark, data_source_name=data_source_path)

# COMMAND ----------

# DBTITLE 1,Create Checks
# The following line can be from a separate YAML file
checks = f"""
checks for {data_source_path}:
- invalid_count(paymentMethod) = 0:
    name: No payment method using AMEX accepted
    attributes:
        check_explanation:  After trade restrictions that happened on 01-01-2025, we're not accepting AMEX cards anymore.
    invalid values: ['amex']

"""

# COMMAND ----------

# DBTITLE 1,Execute Scan
scan.add_sodacl_yaml_str(checks)
#execute the scan
scan.execute()

# COMMAND ----------

# DBTITLE 1,See If Anything Fails
print(scan.get_logs_text())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.sales_transactions_checks