# Gold Layer Data Catalog

## retail_dwh.gold.dim_customers

| Column | Type | Description |
|---|---|---|
| customer_key | bigint | Surrogate key, generated in Gold. Join key for fact_sales. |
| customer_id | string | Original CRM customer id (cst_id). |
| customer_number | string | CRM customer key (cst_key); the join key used to pull in ERP data. |
| first_name | string | Trimmed first name. |
| last_name | string | Trimmed last name. |
| country | string | Standardized country name from ERP location data; 'n/a' if unknown. |
| marital_status | string | 'Single', 'Married', or 'n/a'. |
| gender | string | 'Male', 'Female', or 'n/a'. CRM value wins over ERP on conflict. |
| birthdate | date | From ERP demographic data; null if missing or implausible (future date). |
| create_date | date | Original CRM account creation date. |

## retail_dwh.gold.dim_products

| Column | Type | Description |
|---|---|---|
| product_key | bigint | Surrogate key, generated in Gold. Join key for fact_sales. |
| product_id | string | Original CRM product id. |
| product_number | string | Shortened product key (after the category-id prefix is split off). |
| product_name | string | Product name. |
| category_id | string | Derived from prd_key, underscore-normalized to match ERP category ids. |
| category | string | Category name, from ERP category reference data. |
| subcategory | string | Sub-category name. |
| maintenance | string | Maintenance flag from ERP category reference data. |
| cost | decimal(10,2) | Product cost; 0 where the source was missing a value. |
| product_line | string | 'Road', 'Mountain', 'Other Sales', 'Touring', or 'n/a'. |
| start_date | date | Version start date. Only the current version (prd_end_dt IS NULL) is exposed. |

## retail_dwh.gold.fact_sales

| Column | Type | Description |
|---|---|---|
| order_number | string | Sales order number. Grain: one row per order line item. |
| product_key | bigint | FK to dim_products.product_key. |
| customer_key | bigint | FK to dim_customers.customer_key. |
| order_date | date | Order date. |
| ship_date | date | Ship date. |
| due_date | date | Due date. |
| sales_amount | decimal(10,2) | Line total; always equal to quantity × price after Silver cleansing. |
| quantity | int | Units ordered. |
| price | decimal(10,2) | Unit price. |

## Notes

- All three Gold objects are views (`CREATE OR REPLACE VIEW`), not physical
  tables — they always reflect the latest Silver data with no separate load
  step. (An earlier revision of this project used pandas and had to make
  these physical tables since a DataFrame can't define a view; now that the
  pipeline is back on PySpark, they're views again.)
- Surrogate keys are `ROW_NUMBER()`-generated and are stable only within a
  given query — they are not guaranteed to match prior runs if the
  underlying Silver row order changes.
- The `dlt_pipeline/` folder has an alternative implementation of this same
  schema as Delta Live Tables, with built-in data-quality expectations
  instead of separate validation queries. See that folder's notebook for
  details.
