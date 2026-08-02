# Source Schema Design

Design for the six raw source tables, before any cleaning — this is what
Bronze receives and what Silver's rules are written against. Two source
systems: CRM and ERP, joined on customer/product keys that aren't
consistently formatted between the two (see Relationships below).

## CRM — crm_cust_info

**Grain:** one row per customer *change event* — the same `cst_id` can
appear multiple times as their record is updated. Not one row per customer
(that's what Silver's dedup step produces).

| Column | Type (source) | Description |
|---|---|---|
| cst_id | string | Customer id. Not unique in Bronze — see grain note. |
| cst_key | string | Customer business key. Join key to ERP tables. |
| cst_firstname | string | First name, untrimmed in source. |
| cst_lastname | string | Last name, untrimmed in source. |
| cst_marital_status | string | Single-letter code (`S`/`M`), inconsistent case. |
| cst_gndr | string | Single-letter code (`M`/`F`), inconsistent case. |
| cst_create_date | string | Record creation date. Used to pick the latest row per customer. |

## CRM — crm_prd_info

**Grain:** one row per product *version*. A product can have multiple rows
over time as attributes change; `prd_start_dt` orders them.

| Column | Type (source) | Description |
|---|---|---|
| prd_id | string | Product id. |
| prd_key | string | Compound key: category id (first 5 chars, `-`-delimited) + product number. |
| prd_nm | string | Product name. |
| prd_cost | string | Cost, sometimes missing. |
| prd_line | string | Single-letter product line code. |
| prd_start_dt | string | Version start date. |
| prd_end_dt | string | Version end date — unreliable in source, recomputed in Silver. |

## CRM — crm_sales_details

**Grain:** one row per order line item.

| Column | Type (source) | Description |
|---|---|---|
| sls_ord_num | string | Sales order number. |
| sls_prd_key | string | FK to crm_prd_info.prd_key (post-split, matches the shortened key). |
| sls_cust_id | string | FK to crm_cust_info.cst_id. |
| sls_order_dt | string | `YYYYMMDD` integer-as-string; `0` = no date. |
| sls_ship_dt | string | Same format as sls_order_dt. |
| sls_due_dt | string | Same format as sls_order_dt. |
| sls_sales | string | Line total. Sometimes inconsistent with quantity × price. |
| sls_quantity | string | Units ordered. |
| sls_price | string | Unit price. Sometimes null/non-positive. |

## ERP — erp_cust_az12

**Grain:** one row per customer.

| Column | Type (source) | Description |
|---|---|---|
| cid | string | Customer id, sometimes prefixed `NAS` — join key to crm_cust_info.cst_key after stripping. |
| bdate | string | Birthdate. Occasionally in the future (data error). |
| gen | string | Gender, several spellings (`M`/`Male`/`F`/`Female`). |

## ERP — erp_loc_a101

**Grain:** one row per customer.

| Column | Type (source) | Description |
|---|---|---|
| cid | string | Customer id, sometimes hyphenated — join key to crm_cust_info.cst_key after stripping hyphens. |
| cntry | string | Country, several spellings (`US`/`USA`, `DE`, blanks). |

## ERP — erp_px_cat_g1v2

**Grain:** one row per category id. Reference/lookup table, not transactional.

| Column | Type (source) | Description |
|---|---|---|
| id | string | Category id — join key to crm_prd_info.cat_id (post-normalization). |
| cat | string | Category name. |
| subcat | string | Sub-category name. |
| maintenance | string | Maintenance flag. |

## Relationships

```
crm_cust_info.cst_key ─┬─── erp_cust_az12.cid   (after stripping "NAS" prefix)
                        └─── erp_loc_a101.cid    (after stripping hyphens)

crm_prd_info.cat_id ────── erp_px_cat_g1v2.id     (after normalizing "-" to "_")

crm_cust_info.cst_id ───── crm_sales_details.sls_cust_id
crm_prd_info.prd_key ───── crm_sales_details.sls_prd_key   (post-split key)
```

Nothing in the raw sources actually enforces these relationships — no
foreign keys, no shared formatting. Every join above only works after the
specific cleaning step noted in parentheses, which is why those steps exist
in Silver in the first place.
