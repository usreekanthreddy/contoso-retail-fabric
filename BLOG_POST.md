# Building an End-to-End Retail Analytics Platform on Microsoft Fabric

*A complete, reproducible walkthrough — from raw CSVs to a secured, refreshable Power BI report — using the Lakehouse medallion pattern, Direct Lake, dynamic row-level security, and a Dev → Prod deployment pipeline.*

> Companion repo: **https://github.com/usreekanthreddy/contoso-retail-fabric**

---

## Why this project

Most "Fabric demos" stop at loading a CSV and drawing a bar chart. Real analytics platforms need more: a layered data architecture you can trust, a dimensional model that performs, security that respects who is looking, and a release process that gets changes from development to production safely.

This post documents a complete build for a fictional retailer, **Contoso Retail 360**. Everything here is reproducible — the repo ships seed data, the notebooks, the SQL validation, and the DAX so you can stand the whole thing up in your own Fabric workspace in well under an hour.

The finished platform answers questions like:

- How are sales trending month over month, and how does that compare to last year?
- Which regions and stores are pulling their weight, and which channel (Store vs. Online) is winning?
- What's our gross margin, and which products drive it?
- …while making sure a regional manager only ever sees their own region's numbers.

---

## The architecture at a glance

The solution follows the **medallion architecture** — three progressively refined layers inside a single Fabric Lakehouse (`lh_retail`):

```
Source CSVs (Files/landing/)
        │   file-based ingest
        ▼
┌─────────────────┐
│  BRONZE          │  raw, as-landed, + ingest metadata
│  *_raw tables    │  (_ingest_ts, _source_file)
└─────────────────┘
        │   clean, cast, de-dupe, hash PII
        ▼
┌─────────────────┐
│  SILVER          │  conformed, typed business entities
│  sales/products  │  customers (FullName hashed)
│  stores/customers│
└─────────────────┘
        │   build star schema, surrogate keys
        ▼
┌─────────────────┐
│  GOLD            │  DimDate / DimProduct / DimStore /
│  star schema     │  DimCustomer / DimChannel + FactSales
│  (+ UserRegion)  │  Delta change-data-feed ON
└─────────────────┘
        │   Direct Lake
        ▼
   Semantic model  →  Power BI report (with RLS)
```

**Why these choices:**

- **Bronze / Silver / Gold** keeps raw data immutable and auditable, isolates cleaning logic, and gives BI a stable, well-shaped surface to build on. If a transform is wrong, you fix Silver/Gold and re-run — Bronze is untouched.
- **Direct Lake** lets Power BI read the Gold Delta tables directly from OneLake — no import refresh, no DirectQuery latency. You get import-like speed with live data.
- **Delta change data feed** on the Gold tables makes Direct Lake "framing" (the refresh that picks up new data) efficient — only changed files are reprocessed.

---

## The data model: a classic star schema

Gold is a textbook star: one fact table surrounded by conformed dimensions.

**`FactSales`** (one row per order line)

| Column | Notes |
|---|---|
| `SalesKey` | surrogate key |
| `DateKey`, `ProductKey`, `StoreKey`, `CustomerKey`, `ChannelKey` | foreign keys to dimensions |
| `OrderNumber` | degenerate dimension (for distinct order counts) |
| `Quantity`, `UnitPrice`, `DiscountAmount` | additive measures |
| `SalesAmount` | `Quantity × UnitPrice − DiscountAmount` |
| `UnitCost` | carried for margin math |

**Dimensions**

- `DimDate` — a generated calendar with Day/Month/Quarter/Year, MonthName, DayOfWeek, IsWeekend. Marked as the date table to unlock time-intelligence.
- `DimProduct` — product, category, subcategory, brand, list price, unit cost.
- `DimStore` — store, city, region, country, format, open date.
- `DimCustomer` — segment, city, loyalty tier, signup date (name hashed for privacy).
- `DimChannel` — Store / Online.

All relationships are single-direction, one-to-many from each dimension to `FactSales`.

---

## Step 1 — Create the Lakehouse and schemas

In the Fabric workspace, create a Lakehouse named **`lh_retail`** and three schemas: `bronze`, `silver`, `gold`. The notebooks create the schemas defensively too:

```python
for s in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
```

---

## Step 2 — Land the source data

The kit ships four seed CSVs (generated deterministically with `seed=42`, so everyone gets identical numbers):

- `sales.csv` — 3,001 order lines (Dec 2025 – May 2026)
- `products.csv` — 30 products
- `stores.csv` — 12 stores across West / Central / East
- `customers.csv` — 500 customers

Upload them to the Lakehouse under **`Files/landing/`**. In production this folder would be the drop zone for a Data Factory Copy pipeline (`pl_ingest_bronze`); the notebook plays that role for the quick-test path.

---

## Step 3 — Bronze: file-based ingestion

Bronze reads the landing CSVs and writes them straight to Delta tables, stamping each row with ingest metadata so you always know where a record came from and when it arrived:

```python
def read_csv(name):
    return (spark.read.option("header", True).option("inferSchema", True)
            .csv(f"Files/landing/{name}.csv")
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.lit(f"Files/landing/{name}.csv")))

W = lambda df, tbl: (df.write.format("delta")
                       .mode("overwrite")
                       .option("overwriteSchema", "true")
                       .saveAsTable(tbl))

W(read_csv("products"),  "bronze.products_raw")
W(read_csv("stores"),    "bronze.stores_raw")
W(read_csv("customers"), "bronze.customers_raw")
W(read_csv("sales"),     "bronze.sales_raw")
```

`mode("overwrite")` means every notebook is safe to re-run from scratch — no duplicate-on-rerun surprises.

---

## Step 4 — Silver: clean, type, de-duplicate, and protect PII

Silver is where raw becomes trustworthy. Strings become dates and decimals, duplicates are dropped on natural keys, and — importantly — **customer names are hashed and the raw value dropped** before anything sensitive reaches the analytics layer:

```python
# Sales: type-cast, default discounts, de-dupe on the natural key
sl = (spark.read.table("bronze.sales_raw")
      .withColumn("OrderDate", F.to_date("OrderDate"))
      .withColumn("Quantity", F.col("Quantity").cast("int"))
      .withColumn("UnitPrice", F.col("UnitPrice").cast("decimal(18,2)"))
      .withColumn("DiscountAmount",
                  F.coalesce(F.col("DiscountAmount").cast("decimal(18,2)"), F.lit(0)))
      .dropDuplicates(["OrderNumber", "ProductId"]))
W(sl.drop("_ingest_ts", "_source_file"), "silver.sales")

# Customers: SHA-256 the name, then drop the raw column
W(spark.read.table("bronze.customers_raw")
    .withColumn("SignupDate", F.to_date("SignupDate"))
    .withColumn("FullNameHash", F.sha2(F.col("FullName"), 256))
    .drop("FullName", "_ingest_ts", "_source_file")
    .dropDuplicates(["CustomerId"]), "silver.customers")
```

The hash preserves the ability to count and join on a customer without ever exposing their name downstream — a small change that makes the platform far more defensible from a privacy standpoint.

---

## Step 5 — Gold: build the star schema

Gold assembles dimensions with surrogate keys and joins everything into `FactSales`.

```python
# Surrogate-key helper
def skey(df, name):
    return df.withColumn(name, F.row_number().over(
        Window.orderBy(F.monotonically_increasing_id())))

W(skey(spark.read.table("silver.products"),  "ProductKey"),  "gold.DimProduct")
W(skey(spark.read.table("silver.stores"),    "StoreKey"),    "gold.DimStore")
W(skey(spark.read.table("silver.customers"), "CustomerKey"), "gold.DimCustomer")
W(spark.createDataFrame([(1, "Store"), (2, "Online")],
                        ["ChannelKey", "Channel"]), "gold.DimChannel")
```

The date dimension is generated from the actual min/max order dates in the data:

```python
b = (spark.read.table("silver.sales")
        .agg(F.min("OrderDate").alias("mn"), F.max("OrderDate").alias("mx"))
        .collect()[0])
dd = spark.sql(
    f"SELECT explode(sequence(to_date('{b.mn}'), to_date('{b.mx}'), "
    f"interval 1 day)) AS Date")
dd = (dd.withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))
        .withColumn("Year", F.year("Date")).withColumn("Quarter", F.quarter("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("MonthName", F.date_format("Date", "MMMM"))
        .withColumn("DayOfWeek", F.date_format("Date", "EEEE"))
        .withColumn("IsWeekend", F.dayofweek("Date").isin([1, 7])))
W(dd, "gold.DimDate")
```

And the fact table resolves every natural key to a surrogate key, computes `SalesAmount`, and selects the final shape:

```python
fact = (s.join(dp, "ProductId", "left").join(ds, "StoreId", "left")
         .join(dc, "CustomerId", "left").join(dch, "Channel", "left")
         .withColumn("DateKey", F.date_format("OrderDate", "yyyyMMdd").cast("int"))
         .withColumn("SalesAmount",
                     (F.col("Quantity") * F.col("UnitPrice")
                      - F.col("DiscountAmount")).cast("decimal(18,2)"))
         .select(F.monotonically_increasing_id().alias("SalesKey"),
                 "DateKey", "ProductKey", "StoreKey", "CustomerKey", "ChannelKey",
                 "OrderNumber", "Quantity", "UnitPrice", "DiscountAmount",
                 "SalesAmount", "UnitCost"))
W(fact, "gold.FactSales")
```

### A security table for dynamic RLS

Gold also includes a small mapping table that drives **dynamic row-level security** — which user can see which region:

```python
user_region = spark.createDataFrame([
    ("sreekanth@vkn2k.onmicrosoft.com",       "West"),
    ("east.manager@vkn2k.onmicrosoft.com",    "East"),
    ("central.manager@vkn2k.onmicrosoft.com", "Central"),
], ["Email", "Region"])
W(user_region, "gold.UserRegion")
```

### Turn on change data feed

Finally, enable Delta change data feed on every Gold table so Direct Lake refreshes stay incremental and fast:

```python
for t in ["DimProduct","DimStore","DimCustomer","DimChannel",
          "DimDate","UserRegion","FactSales"]:
    spark.sql(f"ALTER TABLE gold.{t} "
              f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
```

---

## Step 6 — Validate the Gold layer

Trust, but verify. The notebook prints a built-in validation block, and `sql/validate_gold.sql` runs the same checks against the SQL analytics endpoint. With the shipped seed data you should get **exactly** these numbers:

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
| Raw `FullName` present in Gold | **False** |

Sales by region and channel (rounded):

| Region | Online | Store |
|---|---|---|
| Central | 224,875 | 317,640 |
| East | 226,002 | 356,374 |
| West | 226,808 | 322,167 |

The **orphan check** is the most important line: it confirms every fact row resolved to a real product, store, customer, and channel. Zero orphans means the joins are clean.

```python
orphans = f.filter(
    F.col("ProductKey").isNull() | F.col("StoreKey").isNull()
    | F.col("CustomerKey").isNull() | F.col("ChannelKey").isNull()).count()
assert orphans == 0
```

---

## Step 7 — Build the Direct Lake semantic model

From the Lakehouse SQL analytics endpoint, choose **New semantic model** and select only the `gold.*` tables. Then wire up the relationships — all single-direction, one-to-many from dimension to fact:

- `DimDate[DateKey]` → `FactSales[DateKey]`
- `DimProduct[ProductKey]` → `FactSales[ProductKey]`
- `DimStore[StoreKey]` → `FactSales[StoreKey]`
- `DimCustomer[CustomerKey]` → `FactSales[CustomerKey]`
- `DimChannel[ChannelKey]` → `FactSales[ChannelKey]`

**Mark `DimDate` as the date table** (on `DimDate[Date]`) — this is what makes time-intelligence work.

---

## Step 8 — Add the DAX measures

The model ships with a full measure library. The base measures:

```dax
Total Sales   = SUM ( FactSales[SalesAmount] )
Total Units   = SUM ( FactSales[Quantity] )
Total Cost    = SUMX ( FactSales, FactSales[Quantity] * FactSales[UnitCost] )
Gross Margin  = [Total Sales] - [Total Cost]
Gross Margin %= DIVIDE ( [Total Sales] - [Total Cost], [Total Sales] )
Order Count   = DISTINCTCOUNT ( FactSales[OrderNumber] )
Avg Basket Size = DIVIDE ( [Total Sales], [Order Count] )
```

> **A note on a bug worth remembering:** define `Gross Margin %` as a *self-contained* expression — `DIVIDE([Total Sales] - [Total Cost], [Total Sales])` — rather than referencing `[Gross Margin]`. During the build, having `%` depend on the plain `Gross Margin` measure (and accidentally overwriting one with the other) produced a circular-dependency error. Making the percentage stand on its own avoids the trap.

Time-intelligence (these are why `DimDate` had to be marked):

```dax
Sales LY   = CALCULATE ( [Total Sales], SAMEPERIODLASTYEAR ( DimDate[Date] ) )
Sales YoY  = [Total Sales] - [Sales LY]
Sales YoY %= DIVIDE ( [Sales YoY], [Sales LY] )
Sales MTD  = CALCULATE ( [Total Sales], DATESMTD ( DimDate[Date] ) )
Sales YTD  = CALCULATE ( [Total Sales], DATESYTD ( DimDate[Date] ) )
Sales MoM %= DIVIDE ( [Total Sales] - [Sales PM], [Sales PM] )
```

Mix and ranking:

```dax
Active Customers = DISTINCTCOUNT ( FactSales[CustomerKey] )
Online Sales %   = DIVIDE (
        CALCULATE ( [Total Sales], DimChannel[Channel] = "Online" ),
        [Total Sales] )
Product Sales Rank = RANKX (
        ALLSELECTED ( DimProduct[ProductName] ), [Total Sales], , DESC )
```

---

## Step 9 — Dynamic row-level security

This is where the `UserRegion` table earns its place. Instead of hard-coding a role per region, a single role filters `DimStore` to the regions mapped to the logged-in user:

```dax
[Region] IN
    SELECTCOLUMNS (
        FILTER ( UserRegion, UserRegion[Email] = USERPRINCIPALNAME () ),
        "r", UserRegion[Region]
    )
```

Add one manager's email + region to `gold.UserRegion`, and they automatically see only their slice — no model changes, no new roles. The West manager sees West; the East manager sees East; an admin row could map to all three.

---

## Step 10 — Build the report

Two pages cover the core audience:

- **Executive Overview** — KPI cards (Total Sales, Gross Margin %, Order Count, Active Customers), a sales-over-time line with YoY, a region/channel matrix, and a top-products bar using the rank measure.
- **Store Performance** — store-level table with margin, format breakdown, and a regional map, laid out so the most-scanned numbers sit top-left.

Because the model is Direct Lake, the report reflects new data as soon as the Gold tables are framed — no scheduled import refresh required.

---

## Step 11 — Orchestrate, monitor, and deploy

A platform isn't done when the report renders. Three operational pieces close the loop:

**Orchestration.** A Data Factory pipeline chains ingest → Silver → Gold notebooks and runs on a schedule, so the medallion stays current without manual runs.

**Failure notification.** The pipeline includes an on-failure activity that alerts when a run breaks — you find out from a notification, not from a stakeholder asking why the numbers look stale.

**Dev → Prod deployment pipeline.** Fabric deployment pipelines move the Lakehouse, semantic model, and report from a Development stage to Production with a controlled promotion, instead of editing production artifacts by hand.

The semantic model is also **certified**, signalling to report authors across the tenant that it's the trusted, governed source for retail sales.

---

## What's in the repo

```
contoso-retail-fabric/
├─ README.md
├─ .gitignore
├─ data/landing/         products.csv · stores.csv · customers.csv · sales.csv
├─ notebooks/            nb_01_bronze_ingest · nb_02_silver_transform · nb_03_gold_model
├─ sql/                  validate_gold.sql
└─ semantic-model/       measures.dax
```

Clone it, follow the run order, and compare your validation numbers against the table above. If they match, your pipeline ran correctly end to end.

---

## Lessons learned

A few things that are easy to get wrong and worth doing right the first time:

- **Hash PII in Silver, not later.** Once a name reaches Gold or the semantic model, it's much harder to claw back. Do it at the conforming step.
- **Keep `Gross Margin %` self-contained** to avoid circular-dependency errors between dependent measures.
- **Mark the date table before writing time-intelligence** — `SAMEPERIODLASTYEAR` and friends silently misbehave otherwise.
- **Enable change data feed on Gold** so Direct Lake framing stays incremental as data grows.
- **Make RLS data-driven.** A `UserRegion` mapping table beats a proliferation of hard-coded roles — onboarding a new manager becomes a single row insert.
- **Validate with assertions, not eyeballs.** The orphan check and the fixed expected totals turn "looks right" into "is right."

---

## Wrapping up

What started as raw CSVs is now a governed analytics platform: layered and auditable, modeled for performance, secured per-user, monitored, and promotable from dev to prod. The same pattern scales well beyond a demo — swap the seed CSVs for a real Copy pipeline, point Bronze at your sources, and the rest of the medallion carries the weight.

The full, reproducible kit lives at **https://github.com/usreekanthreddy/contoso-retail-fabric**. Stand it up, break it, and make it yours.

*— sp77.blogpost.com*
