Idempotent Daily Transaction Pipeline

A production-style Databricks and PySpark pipeline designed to safely process daily transaction files without creating duplicates during file re-delivery or job retries.

The pipeline uses a Control Table for file-level processing tracking and Delta MERGE for transaction-level idempotency, along with Bronze/Silver layers, data quality validation, quarantine handling, and Databricks Workflows.

Tech Stack: Databricks, PySpark, Delta Lake, Unity Catalog, SQL, Databricks Workflows

Key Focus: Reliable, restart-safe, and idempotent data processing.


        Daily CSV File
              ↓
       ┌──────────────┐
       │ Control Table│
       └──────┬───────┘
              ↓
      Already SUCCESS?
         ↙          ↘
       YES           NO
        ↓             ↓
      SKIP        Process File
                      ↓
                 Bronze Layer
                      ↓
                 DQ Validation
                  ↙        ↘
              Valid       Invalid
                ↓            ↓
          Delta MERGE    Quarantine
                ↓
             Silver
