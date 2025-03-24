-- Step 1: Create Schemas
--------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

--=======================================================================================
-- Bronze Layer Start
--=======================================================================================

--------------------------------------------------
-- Bronze Layer: Creating Tables
--------------------------------------------------

-- Table: Customer Information (CRM)
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT PRIMARY KEY,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- Table: Product Information (CRM)
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT PRIMARY KEY,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- Table: Sales Details (CRM)
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50) PRIMARY KEY,
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- Table: ERP Customer Data
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid VARCHAR(50) PRIMARY KEY,
    bdate DATE,
    gen VARCHAR(50)
);

-- Table: ERP Location Data
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

-- Table: ERP Product Categories
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50) PRIMARY KEY,
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

--------------------------------------------------
-- Stored Procedure: Create Bronze Layer Tables
--Usage : CALL bronze.load_bronze();
--------------------------------------------------

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP := NOW();
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE 'Process: Creating Bronze Layer Tables - Start at %', start_time;
    
    -- Create Customer Info Table
    CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
        cst_id INT PRIMARY KEY,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
    );
    
    -- Create Product Info Table
    CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
        prd_id INT PRIMARY KEY,
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    );
    
    -- Create Sales Details Table
    CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
        sls_ord_num  VARCHAR(50) PRIMARY KEY,
        sls_prd_key  VARCHAR(50),
        sls_cust_id  INT,
        sls_order_dt INT,
        sls_ship_dt  INT,
        sls_due_dt   INT,
        sls_sales    INT,
        sls_quantity INT,
        sls_price    INT
    );
    
    -- Create ERP Customer Table
    CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
        cid VARCHAR(50) PRIMARY KEY,
        bdate DATE,
        gen VARCHAR(50)
    );
    
    -- Create ERP Location Table
    CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50)
    );
    
    -- Create ERP Product Categories Table
    CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
        id VARCHAR(50) PRIMARY KEY,
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50)
    );
    
    end_time := NOW();
    RAISE NOTICE 'Process: Creating Bronze Layer Tables - Completed at %', end_time;
    RAISE NOTICE 'Total Execution Time: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
END;
$$;

--------------------------------------------------
-- Execute the Procedure to Create Tables
--------------------------------------------------
CALL bronze.load_bronze();

--------------------------------------------------
-- Verify Data Import
--------------------------------------------------
SELECT * FROM bronze.crm_cust_info LIMIT 10;
SELECT * FROM bronze.crm_prd_info LIMIT 10;
SELECT * FROM bronze.crm_sales_details LIMIT 10;
SELECT * FROM bronze.erp_cust_az12 LIMIT 10;
SELECT * FROM bronze.erp_loc_a101 LIMIT 10;
SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 10;


-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.crm_cust_info' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.crm_cust_info
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.crm_prd_info' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.crm_prd_info
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.crm_sales_details' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.crm_sales_details
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.erp_cust_az12' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.erp_cust_az12
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.erp_loc_a101' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.erp_loc_a101
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ------------------------------------------------------------
-- Check the data loaded into the 'bronze.erp_px_cat_g1v2' table
-- ------------------------------------------------------------
SELECT * 
FROM bronze.erp_px_cat_g1v2
LIMIT 1000;  -- Limit results to 1000 rows to check if data loaded correctly

-- ============================================================
-- These queries ensure the data is properly loaded into the tables
-- for further processing in the Silver layer.
-- ============================================================

--=============================================================
--               Bronze Layer ended
--=============================================================


--=======================================================================================
--               Silver Layer Started
--=======================================================================================


-- ============================================================
--               DDL for Silver Layer Tables
-- ============================================================

-- ------------------------------------------------------------
-- Table for CRM Customer Information
-- Source: "source_crm - cust_info.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_cust_info;  -- Ensure the table does not already exist
CREATE TABLE silver.crm_cust_info (
    cst_id            INT,                    -- Customer ID
    cst_key           VARCHAR(50),            -- Unique Customer Key
    cst_firstname     VARCHAR(50),            -- Customer First Name
    cst_lastname      VARCHAR(50),            -- Customer Last Name
    cst_marital_status VARCHAR(50),           -- Marital Status of the Customer
    cst_gndr          VARCHAR(50),            -- Gender of the Customer
    cst_create_date   DATE,                   -- Customer Creation Date
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ------------------------------------------------------------
-- Table for CRM Product Information
-- Source: "source_crm - prd_info.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_prd_info;  -- Ensure the table does not already exist
CREATE TABLE silver.crm_prd_info (
    prd_id            INT,                    -- Product ID
    prd_key           VARCHAR(50),            -- Product Key
    prd_nm            VARCHAR(50),            -- Product Name
    prd_cost          INT,                    -- Product Cost
    prd_line          VARCHAR(50),            -- Product Line
    prd_start_dt      DATE,                   -- Product Start Date
    prd_end_dt        DATE,                   -- Product End Date
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ------------------------------------------------------------
-- Table for CRM Sales Details
-- Source: "source_crm - sales_details.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_sales_details;  -- Ensure the table does not already exist
CREATE TABLE silver.crm_sales_details (
    sls_ord_num       VARCHAR(50),            -- Sales Order Number
    sls_prd_key       VARCHAR(50),            -- Product Key (linked to product)
    sls_cust_id       INT,                    -- Customer ID (linked to customer)
    sls_order_dt      INT,                    -- Sales Order Date (as integer)
    sls_ship_dt       INT,                    -- Sales Shipment Date (as integer)
    sls_due_dt        INT,                    -- Sales Due Date (as integer)
    sls_sales         INT,                    -- Sales Amount
    sls_quantity      INT,                    -- Quantity Sold
    sls_price         INT,                    -- Price per Unit
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ------------------------------------------------------------
-- Table for ERP Customer Information AZ12
-- Source: "source_erp - CUST_AZ12.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_cust_az12;  -- Ensure the table does not already exist
CREATE TABLE silver.erp_cust_az12 (
    cid               VARCHAR(50),            -- Customer ID
    bdate             DATE,                   -- Birthdate of the Customer
    gen               VARCHAR(50),            -- Gender of the Customer
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ------------------------------------------------------------
-- Table for ERP Location Information (A101)
-- Source: "source_erp - LOC_A101.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_loc_a101;  -- Ensure the table does not already exist
CREATE TABLE silver.erp_loc_a101 (
    cid               VARCHAR(50),            -- Customer ID
    cntry             VARCHAR(50),            -- Country of the Customer
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ------------------------------------------------------------
-- Table for ERP PX Category G1V2 Information
-- Source: "source_erp - PX_CAT_G1V2.csv"
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;  -- Ensure the table does not already exist
CREATE TABLE silver.erp_px_cat_g1v2 (
    id                VARCHAR(50),            -- Unique ID
    cat               VARCHAR(50),            -- Category
    subcat            VARCHAR(50),            -- Sub-category
    maintenance      VARCHAR(50),            -- Maintenance Info
    dwh_create_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Data Warehouse Record Creation Date (automatically set)
);

-- ============================================================
-- Successfully executed and created tables for the Silver Layer
-- ============================================================





--=======================================================================================
-- Procedure to Load Data into Silver Layer from Bronze Layer
-- Procedure: silver.load_silver
-- Purpose: This procedure will process data from the bronze layer, perform necessary 
-- transformations, and load the cleaned data into the silver layer.
-- Usage: CALL silver.load_silver();
--=======================================================================================

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN

    -- =====================================================================================
    -- Step 1: Load Customer Information into silver.crm_cust_info
    -- Purpose: Truncate the existing data and insert the latest cleaned customer data 
    -- with transformed values for marital status and gender.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key, 
        TRIM(cst_firstname) AS cst_firstname, 
        TRIM(cst_lastname) AS cst_lastname, 
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a' 
        END cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a' 
        END AS cst_gndr, 
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS subquery
    WHERE flag_last = 1;

    -- =====================================================================================
    -- Step 2: Load Product Information into silver.crm_prd_info
    -- Purpose: Truncate and insert the latest product data with cleaned product keys 
    -- and calculated end dates using window functions.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.crm_prd_info;
    
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Cleaned category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,  -- Extracted product key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            ELSE 'n/a' 
        END AS prd_line,
        prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_nm ORDER BY prd_start_dt) - INTERVAL '1 day' AS date) AS prd_end_dt
    FROM bronze.crm_prd_info;

    -- =====================================================================================
    -- Step 3: Load Sales Details into silver.crm_sales_details
    -- Purpose: Truncate and insert sales data, ensuring the correct transformation for 
    -- order dates and recalculating sales and price based on quantity and price.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR(20))) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR(20)) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR(20))) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR(20)) AS DATE)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR(20))) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR(20)) AS DATE)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity THEN sls_quantity * ABS(sls_price) 
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    -- =====================================================================================
    -- Step 4: Load Customer Data from ERP System into silver.erp_cust_az12
    -- Purpose: Truncate and insert customer data from the ERP system, with corrections 
    -- for invalid birth dates and gender transformations.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
        SUBSTRING(cid, 4, LENGTH(cid)) AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    -- =====================================================================================
    -- Step 5: Load Location Data from ERP System into silver.erp_loc_a101
    -- Purpose: Truncate and insert location data, correcting country codes to full names.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.erp_loc_a101;
    
    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    -- =====================================================================================
    -- Step 6: Load Category Data from ERP System into silver.erp_px_cat_g1v2
    -- Purpose: Truncate and insert product category data from the ERP system.
    -- =====================================================================================
    
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

END;
$$;


-- ========================================================================================
-- Execute the procedure to load the silver layer data
-- Usage: CALL silver.load_silver();
-- ========================================================================================
CALL silver.load_silver();




--=======================================================================================
-- Data Quality Checks : 
--=======================================================================================



-- ============================================================
--        Data Quality Verification for silver.crm_cust_info Table
-- ============================================================

-- ------------------------------------------------------------
-- Check if there are any records with unwanted spaces in firstname, lastname, gender, or marital status
-- ------------------------------------------------------------
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname 
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr 
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_marital_status 
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- ------------------------------------------------------------
-- Check for Duplicates or Nulls in Primary Key (cst_id)
-- Expectation: No duplicates or NULL values in cst_id
-- ------------------------------------------------------------
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- ------------------------------------------------------------
-- Verify Data after Transformation (Check for Proper Data Insertion)
-- ------------------------------------------------------------
SELECT *
FROM silver.crm_cust_info;

-- ============================================================
-- This script ensures that after inserting data into the silver layer, 
-- we verify that there are no unwanted spaces in key columns and 
-- no duplicates or NULL values in the primary key column.
-- ============================================================



--=======================================================================================
-- Data Quality Checks for silver.crm_prd_info
-- Purpose: Verify data quality in product information.
--=======================================================================================

-- Check for duplicate or NULL prd_id values
SELECT prd_id, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for leading/trailing spaces in prd_nm
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for invalid prd_cost values (less than 1 or NULL)
SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 1 OR prd_cost IS NULL;

-- Check for rows where prd_end_dt is earlier than prd_start_dt
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check distinct prd_line values to ensure consistency
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info;

--=======================================================================================
-- Data Quality Checks Completed for silver.crm_prd_info
--=======================================================================================



--=======================================================================================
-- Data Quality Check for silver.crm_sales_details
-- Purpose: Verify data consistency and correctness for sales details.
--=======================================================================================

-- Check for invalid order, ship, or due dates (order date should not be greater than ship or due dates)
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check if sls_sales matches the product of sls_price and sls_quantity
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity;

-- Check for non-positive sales, quantity, or price
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;

--=======================================================================================
-- Data Quality Check Completed for crm_sales_details
--=======================================================================================




--=======================================================================================
-- Data Quality Check for silver.erp_cust_az12
-- Purpose: Verify customer data consistency in the ERP system.
--=======================================================================================

-- Check all customer data in the silver.erp_cust_az12 table
SELECT * 
FROM silver.erp_cust_az12;

-- Check distinct gender values in the silver.erp_cust_az12 table
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;

-- Check for invalid birth dates (dates greater than the current date)
SELECT bdate 
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;

--=======================================================================================
-- Data Quality Check Completed for erp_cust_az12
--=======================================================================================




--=======================================================================================
-- Data Quality Check for silver.erp_loc_a101
-- Purpose: Verify the quality of location data after loading into the silver layer.
--=======================================================================================

-- Check for rows with NULL 'cid' values in the silver.erp_loc_a101 table
SELECT * 
FROM silver.erp_loc_a101
WHERE cid IS NULL;

-- Check distinct country values in the silver.erp_loc_a101 table
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101;

--=======================================================================================
-- Data Quality Check Completed for silver.erp_loc_a101
-- Data has been successfully loaded into the silver layer.
--=======================================================================================




--=======================================================================================
-- Data Quality Check after Inserting Data into silver.erp_px_cat_g1v2
-- Purpose: Verify the data quality and ensure consistency after the data is loaded into 
-- the silver layer.
--=======================================================================================

-- Check all data in the silver.erp_px_cat_g1v2 table
SELECT * 
FROM silver.erp_px_cat_g1v2;

-- Check if there are any leading or trailing spaces in 'cat', 'subcat', or 'maintenance' columns
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Check distinct values in the 'maintenance' column for consistency
SELECT DISTINCT maintenance 
FROM silver.erp_px_cat_g1v2;

-- Check distinct values in the 'cat' column to ensure valid categories
SELECT DISTINCT cat 
FROM silver.erp_px_cat_g1v2;

-- Check distinct values in the 'subcat' column to ensure valid subcategories
SELECT DISTINCT subcat 
FROM silver.erp_px_cat_g1v2;

--=======================================================================================
-- Data Quality Check Completed for silver.erp_px_cat_g1v2
-- Data has been successfully loaded and verified for quality in the silver layer.
--=======================================================================================

--=======================================================================================
--Gold Layer
--=======================================================================================

--==========================================================================================
-- Dimension Creation for Gold Layer in PostgreSQL
-- Purpose: The following scripts create the necessary dimensional views in the gold layer,
-- which include customer, product, and sales data. These views provide a summarized, 
-- cleaned, and transformed version of data from the silver layer.
--==========================================================================================

--==========================================================================================
-- Step 1: Create Customer Dimension - gold.dim_customer
-- Purpose: This view will transform and load customer-related data, combining customer 
-- information from the CRM system and ERP system (gender, marital status, and location).
--==========================================================================================

DROP VIEW IF EXISTS gold.dim_customer;
CREATE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,  -- Creating a unique customer key
    ci.cst_id AS customer_id,                                 -- Customer ID from CRM
    ci.cst_key AS customer_number,                             -- Customer key from CRM
    ci.cst_firstname AS first_name,                            -- Customer first name
    ci.cst_lastname AS last_name,                              -- Customer last name
    la.cntry AS country,                                       -- Customer country from ERP
    ci.cst_marital_status AS marital_status,                   -- Marital status from CRM
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- Gender from CRM as the master
        ELSE COALESCE(ca.gen, 'n/a')                    -- If gender is not available in CRM, fallback to ERP
    END AS gender,
    ca.bdate AS birthdate,                                     -- Birthdate from ERP
    ci.cst_create_date AS create_date                          -- Customer creation date from CRM
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid  -- Joining ERP customer data
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;   -- Joining ERP location data

--==========================================================================================
-- Step 2: Create Product Dimension - gold.dim_products
-- Purpose: This view will transform and load product-related data, combining product 
-- information from the CRM and ERP system (product categories, subcategories, and maintenance).
--==========================================================================================

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- Unique product key
    pn.prd_id AS product_id,                                                 -- Product ID from CRM
    pn.prd_key AS product_number,                                             -- Product key from CRM
    pn.prd_nm AS product_name,                                               -- Product name from CRM
    pn.cat_id AS category_id,                                                -- Product category ID
    pc.cat AS category,                                                      -- Product category name from ERP
    pc.subcat AS subcategory,                                                -- Product subcategory from ERP
    pc.maintenance,                                                          -- Maintenance flag from ERP
    pn.prd_cost AS cost,                                                     -- Product cost from CRM
    pn.prd_line AS product_line,                                             -- Product line from CRM
    pn.prd_start_dt AS start_date                                            -- Product start date from CRM
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id  -- Joining ERP category data
WHERE pn.prd_end_dt IS NULL;  -- Ensuring the product end date is null (active products)

--==========================================================================================
-- Step 3: Create Fact Sales View - gold.fact_sales
-- Purpose: This view will summarize and load sales-related data, combining sales information 
-- from the CRM system with the newly created product and customer dimension data.
--==========================================================================================

DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,  -- Sales order number from CRM
    pr.product_key,                   -- Product key from the product dimension
    cu.customer_key,                  -- Customer key from the customer dimension
    sd.sls_order_dt AS order_date,    -- Sales order date from CRM
    sd.sls_ship_dt AS shipping_date,  -- Sales shipping date from CRM
    sd.sls_due_dt AS due_date,        -- Sales due date from CRM
    sd.sls_sales AS sales,            -- Sales amount from CRM
    sd.sls_quantity AS quantity,      -- Sales quantity from CRM
    sd.sls_price AS price             -- Sales price from CRM
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number  -- Joining product dimension
LEFT JOIN gold.dim_customer cu ON sd.sls_cust_id = cu.customer_id;  -- Joining customer dimension

--==========================================================================================
-- Summary
-- Purpose: The above views have been created for the Gold layer to optimize reporting and 
-- analytics. These views join data from multiple source tables (CRM, ERP) and provide a 
-- clean, consolidated dataset for downstream processing, including sales, customer, 
-- and product data.
--==========================================================================================



--===============================================================================
--Quality Checks on gold layer
--===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.


--===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customer'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customer
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  



-- -------------------------------------------------------------
-- EXPLORATORY DATA ANALYSIS (EDA) USING SQL
-- DATABASE EXPLORATION
-- -------------------------------------------------------------

-- Explore the structure of the database: List all tables
SELECT * 
FROM information_schema.tables;  -- Explore all the tables in the database

-- Explore columns in a specific table (e.g., dim_customer)
SELECT * 
FROM information_schema.columns 
WHERE table_name = 'dim_customer';  -- List all columns in 'dim_customer' table

-- -------------------------------------------------------------
-- DIMENSION EXPLORATION
-- -------------------------------------------------------------

-- Explore all countries in the business (dimension: customer)
SELECT DISTINCT country 
FROM gold.dim_customer;  -- List distinct countries in our business

-- Explore all product categories in the business (dimension: products)
SELECT DISTINCT category, subcategory, product_name
FROM gold.dim_products
ORDER BY 1, 2, 3;  -- List distinct categories, subcategories, and product names

-- -------------------------------------------------------------
-- DATE EXPLORATION
-- -------------------------------------------------------------

-- Explore the range of order dates
SELECT 
    MIN(order_date) AS first_order_date,  -- Find the first order date
    MAX(order_date) AS last_order_date,   -- Find the last order date
    EXTRACT(YEAR FROM age(MAX(order_date), MIN(order_date))) AS order_range_years  -- Calculate order range in years
FROM gold.fact_sales;

-- Explore the oldest and youngest customers by birthdate
SELECT
    MIN(birthdate) AS oldest_birthdate,  -- Find the oldest birthdate
    EXTRACT(YEAR FROM age(MIN(birthdate), CURRENT_DATE)) AS oldest_age,  -- Calculate age of oldest customer
    MAX(birthdate) AS youngest_birthdate,  -- Find the youngest birthdate
    EXTRACT(YEAR FROM age(MAX(birthdate), CURRENT_DATE)) AS youngest_age  -- Calculate age of youngest customer
FROM gold.dim_customer;

-- -------------------------------------------------------------
-- MEASURES EXPLORATION
-- -------------------------------------------------------------

-- Find total sales in the business
SELECT SUM(price) AS total_Sales 
FROM gold.fact_sales;  -- Total sales value

-- Find total quantity of items sold
SELECT SUM(quantity) AS total_quantity 
FROM gold.fact_sales;  -- Total quantity sold

-- Find the average selling price
SELECT AVG(price) AS avg_price 
FROM gold.fact_sales;  -- Average price of sold items

-- Find the total number of orders
SELECT COUNT(DISTINCT order_number) AS total_orders 
FROM gold.fact_sales;  -- Total distinct orders

-- Find total number of products
SELECT COUNT(DISTINCT product_id) AS total_products 
FROM gold.dim_products;  -- Total distinct products

-- Find total number of customers
SELECT COUNT(DISTINCT customer_id) AS total_customers 
FROM gold.dim_customer;  -- Total distinct customers

-- Find total number of customers who placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers 
FROM gold.fact_sales;  -- Total distinct customers who made a purchase

-- -------------------------------------------------------------
-- REPORT GENERATION
-- -------------------------------------------------------------

-- Generate a report of key measures
SELECT 'Total Sales' AS measure_names, SUM(price) AS measure_value 
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) AS measure_value 
FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) AS measure_value 
FROM gold.fact_sales
UNION ALL
SELECT 'Total no. of orders', COUNT(DISTINCT order_number) AS measure_value 
FROM gold.fact_sales
UNION ALL
SELECT 'Total Product', COUNT(DISTINCT product_id) AS measure_value 
FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(DISTINCT customer_id) AS measure_value 
FROM gold.dim_customer
UNION ALL
SELECT 'Total customer ordered', COUNT(DISTINCT customer_key) AS measure_value 
FROM gold.fact_sales;

-- -------------------------------------------------------------
-- MAGNITUDE ANALYSIS
-- -------------------------------------------------------------

-- Find total customers by country
SELECT country, COUNT(DISTINCT customer_id) AS total_customers 
FROM gold.dim_customer
GROUP BY country
ORDER BY total_customers DESC;  -- Total customers grouped by country

-- Find total customers by gender
SELECT gender, COUNT(DISTINCT customer_id) AS total_customers 
FROM gold.dim_customer
GROUP BY gender
ORDER BY total_customers DESC;  -- Total customers grouped by gender

-- Find total products by category
SELECT category, COUNT(DISTINCT product_id) AS total_products 
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;  -- Total products grouped by category

-- Find average cost of products in each category
SELECT category, ROUND(AVG(cost), 2) AS average_cost 
FROM gold.dim_products
GROUP BY category
ORDER BY average_cost DESC;  -- Average cost per category, ordered by cost

-- Find total revenue generated for each product category
SELECT dp.category, SUM(sales) AS total_revenue 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC;  -- Total revenue by category

-- Find total revenue generated by each customer
SELECT dc.customer_id, dc.first_name, SUM(sales) AS total_revenue_per_customer 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.first_name
ORDER BY total_revenue_per_customer DESC;  -- Total revenue by each customer

-- Find the distribution of sold items across countries
SELECT dc.country, SUM(quantity) AS total_sold_items
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_sold_items DESC;  -- Total items sold grouped by country

-- -------------------------------------------------------------
-- RANKING ANALYSIS
-- -------------------------------------------------------------

-- Rank products by total revenue and find the top 5 highest revenue products
SELECT dp.category, dp.product_id, dp.product_name, SUM(price) AS total_revenue 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
GROUP BY dp.category, dp.product_id, dp.product_name
ORDER BY total_revenue DESC
LIMIT 5;  -- Limit to the top 5 products

-- Using window function (ROW_NUMBER)
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SUM(price) DESC) AS rank_products,  -- Rank by total revenue (using ROW_NUMBER)
        dp.category,
        dp.product_id,
        dp.product_name,
        SUM(price) AS total_revenue
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
    GROUP BY dp.category, dp.product_id, dp.product_name
) t
WHERE rank_products <= 5;  -- Filter to top 5 ranked products

-- Using window function (RANK)
SELECT *
FROM (
    SELECT
        RANK() OVER (ORDER BY SUM(price) DESC) AS rank_products,  -- Rank by total revenue (using RANK)
        dp.category,
        dp.product_id,
        dp.product_name,
        SUM(price) AS total_revenue
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
    GROUP BY dp.category, dp.product_id, dp.product_name
) t
WHERE rank_products <= 5;  -- Filter to top 5 ranked products

-- -------------------------------------------------------------
-- CUSTOMER ANALYSIS: THE THREE CUSTOMERS WITH THE FEWEST ORDERS
-- -------------------------------------------------------------

-- Rank customers by the fewest orders placed
SELECT *
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY COUNT(fs.customer_key) ASC) AS rank_orders,  -- Rank by fewest orders (ascending)
        dc.first_name,
        dc.customer_id,
        COUNT(fs.customer_key) AS number_of_placed_orders
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_customer dc ON fs.customer_key = dc.customer_key
    GROUP BY dc.customer_id, dc.first_name
) t
WHERE rank_orders <= 3;  -- Filter for the customers with the fewest orders
