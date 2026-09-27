# Databricks notebook source


# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


def process_file(file_name):

    print(f"Starting processing: {file_name}")

    landing_path = "/Volumes/banking_mini/bronze/landing/"
    file_path = landing_path + file_name

    # --------------------------------------------------
    # 1. Check whether file was already successfully processed
    # --------------------------------------------------

    control_df = spark.table(
        "banking_mini.control.file_processing"
    )

    already_processed = (
        control_df
        .filter(
            (F.col("file_name") == file_name) &
            (F.col("status") == "SUCCESS")
        )
        .limit(1)
        .count() > 0
    )

    if already_processed:
        print(f"SKIPPED: {file_name} was already processed.")
        return

    # --------------------------------------------------
    # 2. Read source file
    # --------------------------------------------------

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    records_received = df.count()

    print(f"Records received: {records_received}")

    # --------------------------------------------------
    # 3. Add Bronze metadata
    # --------------------------------------------------

    bronze_df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(file_name))
    )

    # --------------------------------------------------
    # 4. Write Bronze
    # --------------------------------------------------

    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("banking_mini.bronze.transactions")

    # --------------------------------------------------
    # 5. Data Quality validation
    # --------------------------------------------------

    invalid_df = bronze_df.filter(
        F.col("transaction_id").isNull()
        | F.col("customer_id").isNull()
        | (F.col("amount") <= 0)
        | (~F.col("payment_type").isin(
            "UPI",
            "CARD",
            "NET_BANKING"
        ))
    )

    records_rejected = invalid_df.count()

    # --------------------------------------------------
    # 6. Quarantine invalid records
    # --------------------------------------------------

    quarantine_df = (
        invalid_df
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("transaction_id").isNull(),
                "NULL_TRANSACTION_ID"
            )
            .when(
                F.col("customer_id").isNull(),
                "NULL_CUSTOMER_ID"
            )
            .when(
                F.col("amount") <= 0,
                "INVALID_AMOUNT"
            )
            .when(
                ~F.col("payment_type").isin(
                    "UPI",
                    "CARD",
                    "NET_BANKING"
                ),
                "INVALID_PAYMENT_TYPE"
            )
        )
        .withColumn(
            "rejection_timestamp",
            F.current_timestamp()
        )
    )

    if records_rejected > 0:

        quarantine_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(
                "banking_mini.quarantine.transactions"
            )

    # --------------------------------------------------
    # 7. Keep valid records
    # --------------------------------------------------

    valid_df = bronze_df.filter(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("amount") > 0)
        & F.col("payment_type").isin(
            "UPI",
            "CARD",
            "NET_BANKING"
        )
    )

    # --------------------------------------------------
    # 8. Deduplicate within the incoming file
    # --------------------------------------------------

    silver_df = valid_df.dropDuplicates(
        ["transaction_id"]
    )

    records_processed = silver_df.count()

    # --------------------------------------------------
    # 9. Idempotent MERGE into Silver
    # --------------------------------------------------

    silver_table = DeltaTable.forName(
        spark,
        "banking_mini.silver.transactions"
    )

    (
        silver_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # --------------------------------------------------
    # 10. Record successful processing
    # --------------------------------------------------

    control_record = spark.createDataFrame(
        [(
            file_name,
            "SUCCESS",
            records_received,
            records_processed,
            records_rejected
        )],
        [
            "file_name",
            "status",
            "records_received",
            "records_processed",
            "records_rejected"
        ]
    ).withColumn(
        "processed_at",
        F.current_timestamp()
    ).withColumn(
        "error_message",
        F.lit(None).cast("string")
    )

    control_record.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            "banking_mini.control.file_processing"
        )

    print("Processing completed successfully.")
    print(f"Received : {records_received}")
    print(f"Processed: {records_processed}")
    print(f"Rejected : {records_rejected}")

# COMMAND ----------

dbutils.widgets.text("file_name", "")

file_name = dbutils.widgets.get("file_name")

if not file_name:
    raise ValueError("file_name parameter is required")

process_file(file_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     file_name,
# MAGIC     status,
# MAGIC     records_received,
# MAGIC     records_processed,
# MAGIC     records_rejected,
# MAGIC     processed_at
# MAGIC FROM banking_mini.control.file_processing
# MAGIC ORDER BY processed_at;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM banking_mini.silver.transactions;