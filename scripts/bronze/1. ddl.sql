
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
