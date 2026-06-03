-- ============================================================
-- Contoso Retail 360 - Gold layer validation
-- Run against the Lakehouse SQL analytics endpoint (T-SQL).
-- Replace <lakehouse> if your schema/database name differs.
-- ============================================================

-- 1. Row counts per layer (sanity check ingest -> gold)
SELECT 'bronze.sales_raw' AS tbl, COUNT(*) AS rows FROM bronze.sales_raw
UNION ALL SELECT 'silver.sales',    COUNT(*) FROM silver.sales
UNION ALL SELECT 'gold.FactSales',  COUNT(*) FROM gold.FactSales;

-- 2. No orphan dimension keys in the fact (each must return 0)
SELECT
    SUM(CASE WHEN ProductKey  IS NULL THEN 1 ELSE 0 END) AS null_product,
    SUM(CASE WHEN StoreKey    IS NULL THEN 1 ELSE 0 END) AS null_store,
    SUM(CASE WHEN CustomerKey IS NULL THEN 1 ELSE 0 END) AS null_customer,
    SUM(CASE WHEN ChannelKey  IS NULL THEN 1 ELSE 0 END) AS null_channel
FROM gold.FactSales;

-- 3. Referential integrity: fact keys must exist in dimensions
SELECT COUNT(*) AS fact_product_orphans
FROM gold.FactSales f
LEFT JOIN gold.DimProduct d ON f.ProductKey = d.ProductKey
WHERE d.ProductKey IS NULL;

-- 4. SalesAmount must reconcile with Qty * Price - Discount
SELECT COUNT(*) AS amount_mismatches
FROM gold.FactSales
WHERE ABS(SalesAmount - (Quantity * UnitPrice - DiscountAmount)) > 0.01;

-- 5. Headline numbers (eyeball against expectations)
SELECT
    COUNT(DISTINCT OrderNumber) AS orders,
    SUM(Quantity)               AS units,
    CAST(SUM(SalesAmount)      AS decimal(18,2)) AS total_sales,
    CAST(SUM(Quantity*UnitCost) AS decimal(18,2)) AS total_cost,
    CAST(SUM(SalesAmount) - SUM(Quantity*UnitCost) AS decimal(18,2)) AS gross_margin
FROM gold.FactSales;

-- 6. Sales by month (trend smoke test)
SELECT d.Year, d.Month, d.MonthName,
       CAST(SUM(f.SalesAmount) AS decimal(18,2)) AS sales
FROM gold.FactSales f
JOIN gold.DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName
ORDER BY d.Year, d.Month;

-- 7. Top 10 products by sales
SELECT TOP 10 p.ProductName, p.Category,
       CAST(SUM(f.SalesAmount) AS decimal(18,2)) AS sales
FROM gold.FactSales f
JOIN gold.DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.Category
ORDER BY sales DESC;

-- 8. Sales by region and channel
SELECT s.Region, c.Channel,
       CAST(SUM(f.SalesAmount) AS decimal(18,2)) AS sales
FROM gold.FactSales f
JOIN gold.DimStore s   ON f.StoreKey = s.StoreKey
JOIN gold.DimChannel c ON f.ChannelKey = c.ChannelKey
GROUP BY s.Region, c.Channel
ORDER BY s.Region, c.Channel;
