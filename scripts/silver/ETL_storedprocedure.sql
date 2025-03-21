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
