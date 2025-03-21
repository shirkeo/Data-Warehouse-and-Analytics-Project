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

