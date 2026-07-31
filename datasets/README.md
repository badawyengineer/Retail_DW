# /datasets

Drop the six original source files here before uploading them to the
Databricks Volume (`/Volumes/retail_dwh/bronze/raw_files/`):

CRM:
- cust_info.csv
- prd_info.csv
- sales_details.csv

ERP:
- CUST_AZ12.csv
- LOC_A101.csv
- PX_CAT_G1V2.csv

This folder is a placeholder — the actual CSVs weren't part of what was
generated here, only the pipeline that consumes them. Add the real files
(from the project material drive link) before running `01_bronze_ingestion`.
