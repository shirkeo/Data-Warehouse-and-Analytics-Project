--create ddl for tables
--create table for bronze level from folder "source_crm - cust_info.csv"
DROP TABLE IF EXISTS bronze.crm_cust_info; --ensure if table exist or not

Create TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date Date
);

--create table for bronze level from folder "source_crm - prd_info.csv"
DROP TABLE IF EXISTS bronze.crm_prd_info; --ensure if table exist or not
Create TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt Date,
    prd_end_dt Date
);

--create table for bronze level from folder "source_crm - sales_details.csv"
DROP TABLE IF EXISTS bronze.crm_sales_details; --ensure if table exist or not
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


--create table for bronze level from folder "source_erp - CUST_AZ12.csv"
DROP TABLE IF EXISTS bronze.erp_cust_az12; --ensure if table exist or not
CREATE TABLE bronze.erp_cust_az12(
    cid VARCHAR(50),
    bdate Date,
    gen VARCHAR(50)
);

--create table for bronze level from folder "source_erp - LOC_A101.csv"
DROP TABLE IF EXISTS bronze.erp_loc_a101; --ensure if table exist or not
CREATE TABLE bronze.erp_loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

--create table for bronze level from folder "source_erp - PX_CAT_G1V2.csv"
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2; --ensure if table exist or not
CREATE TABLE bronze.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
