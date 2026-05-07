## Step 1: Read All Pipelines and Map Dependencies

orders_etl        → (no upstream dependencies)
inventory_sync    → (no upstream dependencies)
customer_360      → depends on orders_etl
product_catalog   → depends on inventory_sync
daily_reports     → depends on orders_etl, customer_360
data_quality_checks → depends on orders_etl, customer_360, product_catalog
ml_feature_pipeline → depends on customer_360
weekly_analytics  → depends on daily_reports (full week)

```
[orders_etl] ──────────┬──────────────────> [daily_reports]
       │                │                         │
       │                │                         v
       v                │                  [weekly_analytics]
[customer_360] ──────── ┤
       │                │
       │                v
       │         [data_quality_checks]
       │                ^
       v                │
[ml_feature_pipeline]   │
                        │
[inventory_sync] ──> [product_catalog] ────┘
```