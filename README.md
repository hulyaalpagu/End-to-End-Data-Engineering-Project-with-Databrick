# Project Workflow Explanation
In this project, I built an **ETL pipeline in Databricks** using the **medallion architecture (Bronze, Silver, Gold layers)**.

<img width="10000" height="575" alt="Screenshot 2025-09-13 222753" src="https://github.com/user-attachments/assets/914757a9-4211-411e-a24c-118c35bb5c2f" />

**1.Data Ingestion (Bronze Layer Notebook)**

    Read three raw files:

    - Customer.csv

    - Product.json

    - Transaction_snappy.parquet

    Loaded them into the Bronze Layer in Databricks File System (DBFS).

    This layer stores the raw data as-is for traceability and auditing.

**2.Data Transformation & Cleaning (Silver Layer Notebook)**

    Performed transformations, data cleaning, and standardization.

    Combined datasets and prepared them to answer stakeholder KPI questions.

    This layer ensures data is accurate, consistent, and ready for analysis.

**3.Business-Ready Tables (Gold Layer Notebook)**

    Created final curated tables optimized for reporting.

    Produced business insights such as customer behavior, product performance, and transaction summaries.

    These tables are designed for direct use in BI dashboards.

**4.Automation**

    Scheduled the ETL pipeline in Databricks to run every Monday.

    Ensures that data and reports are always refreshed automatically.

**5.Visualization**

   Connected Databricks to Tableau.

   Built interactive dashboards and reports from the Gold Layer tables for stakeholders.

   <img width="999" height="799" alt="Global Sales   Customer Insights Dashboard" src="https://github.com/user-attachments/assets/0d0bd1a5-c501-45ea-b557-cd3feeee153a" />

