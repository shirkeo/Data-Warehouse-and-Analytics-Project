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
