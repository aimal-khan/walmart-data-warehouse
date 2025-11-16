-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT 
    dp.Product_ID,
    dd.year,
    dd.month_name,
    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(fs.total_amount) AS total_revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.year = 2017
GROUP BY dp.Product_ID, dd.year, dd.month, dd.month_name, day_type
ORDER BY day_type, total_revenue DESC
LIMIT 5;

-- Q2: Customer Demographics by Purchase Amount with City Category Breakdown
SELECT 
    dc.Gender,
    dc.Age,
    dc.City_Category,
    SUM(fs.total_amount) AS total_purchase_amount,
    COUNT(DISTINCT dc.Customer_ID) AS customer_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.Gender, dc.Age, dc.City_Category
ORDER BY total_purchase_amount DESC;

-- Q3: Product Category Sales by Occupation
SELECT 
    dc.Occupation,
    dp.Product_Category,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dc.Occupation, dp.Product_Category
ORDER BY dc.Occupation, total_sales DESC;

-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
SELECT 
    dc.Gender,
    dc.Age,
    dd.quarter,
    dd.year,
    SUM(fs.total_amount) AS total_purchases,
    COUNT(fs.sales_key) AS transaction_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.year = 2017
GROUP BY dc.Gender, dc.Age, dd.quarter, dd.year
ORDER BY dd.quarter, dc.Gender, dc.Age;

-- Q5: Top Occupations by Product Category Sales
SELECT *
FROM (
    SELECT 
        dp.Product_Category,
        dc.Occupation,
        SUM(fs.total_amount) AS total_sales,
        RANK() OVER (PARTITION BY dp.Product_Category ORDER BY SUM(fs.total_amount) DESC) AS rank_in_category
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY dp.Product_Category, dc.Occupation
) AS ranked_sales
WHERE rank_in_category <= 5
ORDER BY Product_Category, total_sales DESC;

SELECT MIN(full_date), MAX(full_date) FROM dim_date;
-- Q6: City Category Performance by Marital Status with Monthly Breakdown
--  query using last 6 months relative to that max date
SELECT 
    dc.City_Category,
    dc.Marital_Status,
    dd.year,
    dd.month_name,
    SUM(fs.total_amount) AS purchase_amount
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.full_date >= DATE_SUB(
    (SELECT MAX(full_date) FROM dim_date), INTERVAL 6 MONTH
)
GROUP BY dc.City_Category, dc.Marital_Status, dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month, dc.City_Category;


-- Q7: Average Purchase Amount by Stay Duration and Gender
SELECT 
    dc.Stay_In_Current_City_Years,
    dc.Gender,
    AVG(fs.total_amount) AS avg_purchase_amount,
    COUNT(fs.sales_key) AS transaction_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.Stay_In_Current_City_Years, dc.Gender
ORDER BY dc.Stay_In_Current_City_Years, dc.Gender;

-- Q8: Top 5 Revenue-Generating Cities by Product Category
SELECT *
FROM (
    SELECT 
        dp.Product_Category,
        dc.City_Category,
        SUM(fs.total_amount) AS total_revenue,
        RANK() OVER (
            PARTITION BY dp.Product_Category 
            ORDER BY SUM(fs.total_amount) DESC
        ) AS city_rank
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY dp.Product_Category, dc.City_Category
) AS ranked_cities
WHERE city_rank <= 5
ORDER BY Product_Category, total_revenue DESC;

-- Q9: Monthly Sales Growth by Product Category
WITH monthly_sales AS (
    SELECT 
        dp.Product_Category,
        dd.year,
        dd.month,
        SUM(fs.total_amount) AS monthly_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.year = 2017
    GROUP BY dp.Product_Category, dd.year, dd.month
)
SELECT 
    Product_Category,
    year,
    month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (PARTITION BY Product_Category ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        ((monthly_revenue - LAG(monthly_revenue) OVER (PARTITION BY Product_Category ORDER BY year, month)) / 
        LAG(monthly_revenue) OVER (PARTITION BY Product_Category ORDER BY year, month)) * 100, 2
    ) AS growth_percentage
FROM monthly_sales
ORDER BY Product_Category, year, month;

-- Q10: Weekend vs. Weekday Sales by Age Group
SELECT 
    dc.Age,
    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(fs.total_amount) AS total_sales,
    COUNT(fs.sales_key) AS transaction_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.year = 2017
GROUP BY dc.Age, day_type
ORDER BY dc.Age, day_type;

-- Q11: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT *
FROM (
    SELECT 
        dp.Product_ID,
        dp.Product_Category,
        dd.year,
        dd.month_name,
        CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        SUM(fs.total_amount) AS total_revenue,
        RANK() OVER (
            PARTITION BY dd.year, dd.month, CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY SUM(fs.total_amount) DESC
        ) AS revenue_rank
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.year = 2017
    GROUP BY dp.Product_ID, dp.Product_Category, dd.year, dd.month, dd.month_name, day_type
) AS ranked_products
WHERE revenue_rank <= 5
ORDER BY month_name, day_type, revenue_rank;


-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH quarterly_revenue AS (
    SELECT 
        ds.storeName,
        dd.quarter,
        SUM(fs.total_amount) AS quarterly_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_store ds ON dp.store_key = ds.store_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.year = 2017
    GROUP BY ds.storeName, dd.quarter
)
SELECT 
    storeName,
    quarter,
    quarterly_revenue,
    LAG(quarterly_revenue) OVER (PARTITION BY storeName ORDER BY quarter) AS prev_quarter_revenue,
    ROUND(
        ((quarterly_revenue - LAG(quarterly_revenue) OVER (PARTITION BY storeName ORDER BY quarter)) / 
        LAG(quarterly_revenue) OVER (PARTITION BY storeName ORDER BY quarter)) * 100, 2
    ) AS growth_rate_percentage
FROM quarterly_revenue
ORDER BY storeName, quarter;

-- Q13: Detailed Supplier Sales Contribution by Store and Product Name
SELECT 
    ds.storeName,
    dsup.supplierName,
    dp.Product_ID,
    dp.Product_Category,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_store ds ON dp.store_key = ds.store_key
JOIN dim_supplier dsup ON dp.supplier_key = dsup.supplier_key
GROUP BY ds.storeName, dsup.supplierName, dp.Product_ID, dp.Product_Category
ORDER BY ds.storeName, dsup.supplierName, total_sales DESC;

-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT 
    dp.Product_ID,
    dp.Product_Category,
    dd.season,
    dd.year,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity,
    AVG(fs.total_amount) AS avg_transaction_amount
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.Product_ID, dp.Product_Category, dd.season, dd.year
ORDER BY dp.Product_ID, dd.year, 
         FIELD(dd.season, 'Spring', 'Summer', 'Fall', 'Winter');

-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH monthly_revenue AS (
    SELECT 
        ds.storeName,
        dsup.supplierName,
        dd.year,
        dd.month,
        SUM(fs.total_amount) AS monthly_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_store ds ON dp.store_key = ds.store_key
    JOIN dim_supplier dsup ON dp.supplier_key = dsup.supplier_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY ds.storeName, dsup.supplierName, dd.year, dd.month
)
SELECT 
    storeName,
    supplierName,
    year,
    month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (PARTITION BY storeName, supplierName ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        ABS((monthly_revenue - LAG(monthly_revenue) OVER (PARTITION BY storeName, supplierName ORDER BY year, month)) / 
        LAG(monthly_revenue) OVER (PARTITION BY storeName, supplierName ORDER BY year, month)) * 100, 2
    ) AS volatility_percentage
FROM monthly_revenue
ORDER BY storeName, supplierName, year, month;


-- If this returns 0 rows, your dataset literally has no orders with multiple products → below query correctly returns nothing.
SELECT orderID, COUNT(DISTINCT product_key) AS product_count
FROM fact_sales
GROUP BY orderID
HAVING product_count > 1
LIMIT 10;


-- Q16: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
WITH product_pairs AS (
    SELECT 
        fs1.orderID,
        fs1.product_key AS product1_key,
        fs2.product_key AS product2_key
    FROM fact_sales fs1
    JOIN fact_sales fs2 ON fs1.orderID = fs2.orderID 
        AND fs1.product_key < fs2.product_key
)
SELECT 
    dp1.Product_ID AS Product1_ID,
    dp1.Product_Category AS Product1_Category,
    dp2.Product_ID AS Product2_ID,
    dp2.Product_Category AS Product2_Category,
    COUNT(pp.orderID) AS times_purchased_together
FROM product_pairs pp
JOIN dim_product dp1 ON pp.product1_key = dp1.product_key
JOIN dim_product dp2 ON pp.product2_key = dp2.product_key
GROUP BY dp1.Product_ID, dp1.Product_Category, dp2.Product_ID, dp2.Product_Category
ORDER BY times_purchased_together DESC
LIMIT 5;

-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT 
    COALESCE(ds.storeName, 'ALL STORES') AS storeName,
    COALESCE(dsup.supplierName, 'ALL SUPPLIERS') AS supplierName,
    COALESCE(dp.Product_ID, 'ALL PRODUCTS') AS Product_ID,
    dd.year,
    SUM(fs.total_amount) AS yearly_revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_store ds ON dp.store_key = ds.store_key
JOIN dim_supplier dsup ON dp.supplier_key = dsup.supplier_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, ds.storeName, dsup.supplierName, dp.Product_ID WITH ROLLUP
ORDER BY dd.year, ds.storeName, dsup.supplierName, dp.Product_ID;

-- Q18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
SELECT 
    dp.Product_ID,
    dp.Product_Category,
    dd.year,
    SUM(CASE WHEN dd.month <= 6 THEN fs.total_amount ELSE 0 END) AS H1_revenue,
    SUM(CASE WHEN dd.month > 6 THEN fs.total_amount ELSE 0 END) AS H2_revenue,
    SUM(CASE WHEN dd.month <= 6 THEN fs.quantity ELSE 0 END) AS H1_quantity,
    SUM(CASE WHEN dd.month > 6 THEN fs.quantity ELSE 0 END) AS H2_quantity,
    SUM(fs.total_amount) AS yearly_revenue,
    SUM(fs.quantity) AS yearly_quantity
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.Product_ID, dp.Product_Category, dd.year
ORDER BY dp.Product_ID, dd.year;

-- Q19: Identify High Revenue Spikes in Product Sales and Highlight Outliers

WITH daily_product_sales AS (
    SELECT 
        dp.Product_ID,
        dp.Product_Category,
        dd.full_date,
        SUM(fs.total_amount) AS daily_sales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dp.Product_ID, dp.Product_Category, dd.full_date
),
product_averages AS (
    SELECT 
        Product_ID,
        AVG(daily_sales) AS avg_daily_sales,
        STDDEV(daily_sales) AS stddev_sales
    FROM daily_product_sales
    GROUP BY Product_ID
)
SELECT 
    dps.Product_ID,
    dps.Product_Category,
    dps.full_date,
    dps.daily_sales,
    pa.avg_daily_sales,
    ROUND(dps.daily_sales / pa.avg_daily_sales, 2) AS sales_ratio,
    CASE 
        WHEN dps.daily_sales > 2 * pa.avg_daily_sales THEN 'OUTLIER - High Spike'
        ELSE 'Normal'
    END AS sales_flag
FROM daily_product_sales dps
JOIN product_averages pa ON dps.Product_ID = pa.Product_ID
WHERE dps.daily_sales > 2 * pa.avg_daily_sales
ORDER BY sales_ratio DESC;


-- Previous querie again: part 1
CREATE TEMPORARY TABLE daily_product_sales AS
SELECT 
    dp.Product_ID,
    dp.Product_Category,
    dd.full_date,
    SUM(fs.total_amount) AS daily_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.Product_ID, dp.Product_Category, dd.full_date;

CREATE TEMPORARY TABLE product_averages AS
SELECT 
    Product_ID,
    AVG(daily_sales) AS avg_daily_sales
FROM daily_product_sales
GROUP BY Product_ID;

SELECT 
    dps.Product_ID,
    dps.Product_Category,
    dps.full_date,
    dps.daily_sales,
    pa.avg_daily_sales,
    ROUND(dps.daily_sales / pa.avg_daily_sales, 2) AS sales_ratio,
    CASE 
        WHEN dps.daily_sales > 2 * pa.avg_daily_sales THEN 'OUTLIER - High Spike'
        ELSE 'Normal'
    END AS sales_flag
FROM daily_product_sales dps
JOIN product_averages pa ON dps.Product_ID = pa.Product_ID
WHERE dps.daily_sales > 2 * pa.avg_daily_sales
ORDER BY sales_ratio DESC;



-- Q20: Query the STORE_QUARTERLY_SALES View for Optimized Sales Analysis
SELECT 
    storeName,
    year,
    quarter,
    quarterly_sales,
    LAG(quarterly_sales) OVER (PARTITION BY storeName ORDER BY year, quarter) AS prev_quarter_sales,
    ROUND(
        ((quarterly_sales - LAG(quarterly_sales) OVER (PARTITION BY storeName ORDER BY year, quarter)) / 
        LAG(quarterly_sales) OVER (PARTITION BY storeName ORDER BY year, quarter)) * 100, 2
    ) AS quarter_over_quarter_growth
FROM STORE_QUARTERLY_SALES
ORDER BY storeName, year, quarter;