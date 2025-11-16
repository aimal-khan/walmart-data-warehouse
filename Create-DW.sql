-- Drop existing tables if they exist (in correct order to respect foreign keys)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;

-- Create Dimension Tables
CREATE DATABASE DWProject;
use DWProject;
-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID VARCHAR(50) UNIQUE NOT NULL,
    Gender VARCHAR(10),
    Age VARCHAR(20),
    Occupation INT,
    City_Category VARCHAR(10),
    Stay_In_Current_City_Years VARCHAR(10),
    Marital_Status INT,
    INDEX idx_customer_id (Customer_ID)
);

-- Dimension: Date
CREATE TABLE dim_date (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    season VARCHAR(10),
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year, month),
    INDEX idx_quarter (quarter)
);

-- Dimension: Store
CREATE TABLE dim_store (
    store_key INT AUTO_INCREMENT PRIMARY KEY,
    storeID VARCHAR(50) UNIQUE NOT NULL,
    storeName VARCHAR(100),
    INDEX idx_store_id (storeID)
);

-- Dimension: Supplier
CREATE TABLE dim_supplier (
    supplier_key INT AUTO_INCREMENT PRIMARY KEY,
    supplierID VARCHAR(50) UNIQUE NOT NULL,
    supplierName VARCHAR(100),
    INDEX idx_supplier_id (supplierID)
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(50) UNIQUE NOT NULL,
    Product_Category VARCHAR(100),
    price DECIMAL(10, 2),
    store_key INT,
    supplier_key INT,
    INDEX idx_product_id (Product_ID),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key),
    FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key)
);

-- Fact Table: Sales
CREATE TABLE fact_sales (
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    orderID VARCHAR(50) NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    quantity INT NOT NULL,
    total_amount DECIMAL(12, 2),
    INDEX idx_customer (customer_key),
    INDEX idx_product (product_key),
    INDEX idx_date (date_key),
    INDEX idx_order (orderID),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- Create View for Q20: STORE_QUARTERLY_SALES
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    ds.storeName,
    dd.year,
    dd.quarter,
    SUM(fs.total_amount) AS quarterly_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_store ds ON dp.store_key = ds.store_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY ds.storeName, dd.year, dd.quarter
ORDER BY ds.storeName, dd.year, dd.quarter;