# Contoso Retail 360 — Implementation Kit

Import-ready artifacts for the Microsoft Fabric retail sales analytics project.
Follow the steps below to stand up the full Bronze → Silver → Gold → Power BI
pipeline in your Fabric workspace.

## What's in this kit

```
project/
├─ data/landing/         seed CSVs (upload to Lakehouse Files/landing/)
│   ├─ sales.csv         3,001 order lines · Dec 2025 – May 2026
│   ├─ products.csv      30 products
│   ├─ stores.csv        12 stores across West / Central / East
│   └─ customers.csv     500 customers
├─ notebooks/            import into the Fabric workspace, run in order
│   ├─ nb_01_bronze_ingest.ipynb
│   ├─ nb_02_silver_transform.ipynb
│   └─ nb_03_gold_model.ipynb
├─ sql/validate_gold.sql validation queries for the SQL analytics endpoint
└─ semantic-model/measures.dax   DAX measures for the Direct Lake model
```

## Run order (quick-test path)

1. **Create the Lakehouse.** In your workspace, create a Lakehouse named `lh_retail` with `bronze`, `silver`, `gold` schemas.
2. **Upload seed data.** In the Lakehouse Files area, create a `landing` folder and upload the four CSVs from `data/landing/`.
3. **Import notebooks.** Workspace → Import → Notebook, select the three `.ipynb` files. Attach `lh_retail` as the default Lakehouse.
4. **Run in order:** `nb_01_bronze_ingest` → `nb_02_silver_transform` → `nb_03_gold_model`.
5. **Validate.** Open the Lakehouse SQL analytics endpoint and run `sql/validate_gold.sql`. Compare against the expected numbers below.
6. **Build the semantic model.** From the SQL endpoint choose New semantic model, select the `gold.*` tables, create relationships, mark `DimDate` as the date table, and add the measures from `semantic-model/measures.dax`.
7. **Build the report** on the semantic model.

## Expected validation results (shipped seed data)

| Metric | Expected value |
|---|---|
| Orphan fact rows | 0 |
| Distinct orders | 1,224 |
| Total units | 9,097 |
| Total sales | 1,673,866.79 |
| Total cost | 1,028,681.42 |
| Gross margin | 645,185.37 |
| Gross margin % | 38.5% |
| Date range | 2025-12-01 → 2026-05-31 |

## Semantic model relationships

All single-direction, one-to-many from dimension → `FactSales`:

- `DimDate[DateKey]` → `FactSales[DateKey]`
- `DimProduct[ProductKey]` → `FactSales[ProductKey]`
- `DimStore[StoreKey]` → `FactSales[StoreKey]`
- `DimCustomer[CustomerKey]` → `FactSales[CustomerKey]`
- `DimChannel[ChannelKey]` → `FactSales[ChannelKey]`

Mark `DimDate` as the date table (on `DimDate[Date]`) to enable time-intelligence.

## Notes

- Notebooks use `saveAsTable` with `mode("overwrite")`, so they are safe to re-run.
- For production, hash or drop customer `FullName` in the Silver step (PII), enable Delta change-data-feed on Gold tables, and use a `UserRegion` mapping table for dynamic row-level security.
