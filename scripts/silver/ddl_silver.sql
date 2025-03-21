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
