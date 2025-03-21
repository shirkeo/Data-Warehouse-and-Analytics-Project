---------------------------------------------------------Silver layer------------------------------------------------------------------

SELECT * 
FROM bronze.crm_cust_info
LIMIT 1000;

SELECT *
FROM bronze.crm_prd_info
LIMIT 1000;

SELECT *
FROM bronze.crm_sales_details
LIMIT 1000;

SELECT *
FROM bronze.erp_cust_az12
LIMIT 1000;

SELECT *
FROM bronze.erp_loc_a101
LIMIT 1000;

SELECT *
FROM bronze.erp_px_cat_g1v2
LIMIT 1000;


--DDL-silverlayer

--create ddl for tables
--create table for silver level from folder "source_crm - cust_info.csv"
DROP TABLE IF EXISTS silver.crm_cust_info; --ensure if table exist or not

Create TABLE silver.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date Date,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--create table for silver level from folder "source_crm - prd_info.csv"
DROP TABLE IF EXISTS silver.crm_prd_info; --ensure if table exist or not
Create TABLE silver.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt Date,
    prd_end_dt Date,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--create table for silver level from folder "source_crm - sales_details.csv"
DROP TABLE IF EXISTS silver.crm_sales_details; --ensure if table exist or not
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


--create table for silver level from folder "source_erp - CUST_AZ12.csv"
DROP TABLE IF EXISTS silver.erp_cust_az12; --ensure if table exist or not
CREATE TABLE silver.erp_cust_az12(
    cid VARCHAR(50),
    bdate Date,
    gen VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--create table for silver level from folder "source_erp - LOC_A101.csv"
DROP TABLE IF EXISTS silver.erp_loc_a101; --ensure if table exist or not
CREATE TABLE silver.erp_loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--create table for silver level from folder "source_erp - PX_CAT_G1V2.csv"
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2; --ensure if table exist or not
CREATE TABLE silver.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--successfully exceuted and created tables

--Check for nulls or duplicates in primary key for crm_cust_info
-- Expectation : no result

SELECT cst_id,count(*)
from bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*)>1 or cst_id is NULL;


--used window function to rank as per the row to find duplicates in cst_id. for crm_cust_info
SELECT *,
    row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
from bronze.crm_cust_info
WHERE cst_id = 29466;

SELECT * FROM(
    SELECT *,
        row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info)
WHERE flag_last!=1;


SELECT * FROM(
    SELECT *,
        row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info
        WHERE cst_id is NOT NULL)
WHERE flag_last=1;


--check for unwanted spaces for crm_cust_info table

SELECT cst_firstname from bronze.crm_cust_info
WHERE cst_firstname != trim(cst_firstname)

SELECT cst_lastname from bronze.crm_cust_info
WHERE cst_lastname != trim(cst_lastname)

SELECT cst_gndr from bronze.crm_cust_info
WHERE cst_gndr != trim(cst_gndr)

--transformation of unwanted spaces for crm_cust_info table
SELECT cst_id, cst_key, trim(cst_firstname)as cst_firstname, trim(cst_lastname) as cst_lastname, cst_marital_status as cst_marital_status, cst_gndr, cst_create_date
FROM(
    SELECT *,
        row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info
        WHERE cst_id is NOT NULL)
WHERE flag_last=1;

-- Data standardization & consistency for crm_cust_info table

select distinct(cst_gndr) from bronze.crm_cust_info
-- since we have three values : 'NULL','M','F' and we decide to convert M to Male, F to Female and Deal with Null as per business needs. also dealth with the marital status column.
--transformation
SELECT cst_id,
    cst_key, 
    trim(cst_firstname)as cst_firstname, 
    trim(cst_lastname) as cst_lastname, 
    case 
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'n/a' 
    end as cst_marital_status,
    case 
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'n/a' 
    end cst_gndr, 
    cst_create_date
FROM(
    SELECT *,
        row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info
        WHERE cst_id is NOT NULL)
WHERE flag_last=1;

--now we will insert our transformation to silver layer from bronze layer from crm_cust_info
truncate table silver.crm_cust_info
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
) AS subquery  -- Added alias for subquery
WHERE flag_last = 1;


-----check for unwanted spaces - verifying with silver layer after insert for crm_cust_info table

SELECT * from silver.crm_cust_info

SELECT cst_firstname from silver.crm_cust_info
WHERE cst_firstname != trim(cst_firstname)

SELECT cst_lastname from silver.crm_cust_info
WHERE cst_lastname != trim(cst_lastname)

SELECT cst_gndr from silver.crm_cust_info
WHERE cst_gndr != trim(cst_gndr)

SELECT cst_marital_status from silver.crm_cust_info
WHERE cst_marital_status != trim(cst_marital_status)

SELECT cst_id,count(*)
from silver.crm_cust_info
GROUP BY cst_id
HAVING count(*)>1 or cst_id is NULL;


SELECT * from silver.crm_cust_info

---------------------------- Successfully handled for crm_cust_info --------------------------------

----now dealing with crm_prd_info table

SELECT * FROM bronze.crm_prd_info

--to check if primary key has duplicates or not

SELECT prd_id, count(*) from bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*)>1 or prd_id is null

--adding cat_id column  to crm_prd_info
SELECT prd_id,
    prd_key,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info
WHERE replace(substring(prd_key,1,5),'-','_') not in (select id from bronze.erp_px_cat_g1v2 ) --- while checking we found out that one cat_id is not matching with other table "CO_PE"


SELECT prd_id,
    prd_key,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,  --we created prd_key column here
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info
WHERE substring(prd_key,7,length(prd_key)) in (select sls_prd_key from bronze.crm_sales_details) --- while checking we find out that many keys are not matching with crm_sales_details and but it is fine. for now the transformation is successful. 


SELECT * from bronze.crm_prd_info

--checking unwanted spaces
SELECT prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

--no such unwanted spaces

--check nulls or negative numbers

SELECT prd_cost from bronze.crm_prd_info
where prd_cost < 1 or prd_cost is Null

-- here we deal witht the null values and replacing them with the help of coalesce and replace them '0'
SELECT prd_id,
    prd_key,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,  --we created prd_key column here
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info


SELECT distinct prd_line from bronze.crm_prd_info

--here we deal with prd_line and applybsome standardization

SELECT prd_id,
    prd_key,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,  --we created prd_key column here
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    case when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        when upper(trim(prd_line)) = 'R' then 'Road'
        else 'n/a' 
    end as prd_line ,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info

--succesfully dealth with prd_line

--dealing with prd_start_dt and prd_end_dt. fixing using window function
SELECT prd_id,
    prd_key,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,  --we created prd_key column here
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    case when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        when upper(trim(prd_line)) = 'R' then 'Road'
        else 'n/a' 
    end as prd_line ,
    prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_nm ORDER BY prd_start_dt) - INTERVAL '1 day' as date) AS prd_end_dt  --here we fix the end date using window function.
from bronze.crm_prd_info
where prd_end_dt<prd_start_dt

--no inserting the data to silver layer crm_prd_info but before that we need to fix the table using ddl
--performing DDL
DROP TABLE IF EXISTS silver.crm_prd_info; --ensure if table exist or not
Create TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt Date,
    prd_end_dt Date,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--now inserting
truncate table silver.crm_prd_info
insert into silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id,  --we created a cat_id column since prd_key was holding information for two attributes. and also we can join the tables "erp_px_cat_g1v2" with "crm_prd_info"
    substring(prd_key,7,length(prd_key)) as prd_key,  --we created prd_key column here
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    case when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        when upper(trim(prd_line)) = 'R' then 'Road'
        else 'n/a' 
    end as prd_line ,
    prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_nm ORDER BY prd_start_dt) - INTERVAL '1 day' as date) AS prd_end_dt  --here we fix the end date using window function.
from bronze.crm_prd_info
--succesfully inserted

--verifying the inserted data

--data quality checks starts here
select * from silver.crm_prd_info

SELECT prd_id, count(*) from silver.crm_prd_info
GROUP BY prd_id
HAVING count(*)>1 or prd_id is null

SELECT prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm)

SELECT prd_cost from silver.crm_prd_info
where prd_cost < 1 or prd_cost is Null

SELECT *
from silver.crm_prd_info
where prd_end_dt<prd_start_dt

select distinct prd_line
from silver.crm_prd_info
--data quality check : done


--now handling "crm_sales_details" table 
SELECT * from bronze.crm_sales_details
select cst_id from silver.crm_cust_info

select sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_cust_id not in (SELECT cst_id from silver.crm_cust_info) -- checked prd_key and cst_id 

--checked for invalid dates and resolve

select sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or length(CAST(sls_order_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_order_dt AS VARCHAR(20)) AS DATE)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or length(CAST(sls_ship_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_ship_dt AS VARCHAR(20)) AS DATE)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or length(CAST(sls_due_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_due_dt AS VARCHAR(20)) AS DATE)
    end as sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt  --for quality Check

--finding alot of errors in sls_sales,sls_quantity, sls_price and resolving them.
SELECT
    case 
        when sls_sales is null or sls_sales <= 0 or sls_sales != ABS(sls_price) * sls_quantity 
            then sls_quantity * ABS(sls_price) 
            else sls_price
    end as sls_sales,
    
    sls_quantity,
    
    case 
        when sls_price is null or sls_price <= 0
            then sls_sales/NULLIF(sls_quantity,0)
            else sls_price
    end as sls_price    
    
from bronze.crm_sales_details
WHERE 
    (CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity 
        THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
    END) 
    != 
    sls_quantity * 
    (CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0) 
        ELSE sls_price 
    END);

-- combining all transformation
select sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or length(CAST(sls_order_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_order_dt AS VARCHAR(20)) AS DATE)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or length(CAST(sls_ship_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_ship_dt AS VARCHAR(20)) AS DATE)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or length(CAST(sls_due_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_due_dt AS VARCHAR(20)) AS DATE)
    end as sls_due_dt,
    case 
        when sls_sales is null or sls_sales <= 0 or sls_sales != ABS(sls_price) * sls_quantity 
            then sls_quantity * ABS(sls_price) 
            else sls_sales
    end as sls_sales,
    sls_quantity,
    case 
        when sls_price is null or sls_price <= 0
            then sls_sales/NULLIF(sls_quantity,0)
            else sls_price
    end as sls_price
from bronze.crm_sales_details


-- fixing the table datatype before inserting
DROP TABLE IF EXISTS silver.crm_sales_details; --ensure if table exist or not
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt date,
    sls_ship_dt  date,
    sls_due_dt   date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



--now inserting data
truncate table silver.crm_sales_details
insert into silver.crm_sales_details(
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
select sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or length(CAST(sls_order_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_order_dt AS VARCHAR(20)) AS DATE)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or length(CAST(sls_ship_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_ship_dt AS VARCHAR(20)) AS DATE)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or length(CAST(sls_due_dt AS VARCHAR(20))) != 8 then Null
        else CAST(CAST(sls_due_dt AS VARCHAR(20)) AS DATE)
    end as sls_due_dt,
    case 
        when sls_sales is null or sls_sales <= 0 or sls_sales != ABS(sls_price) * sls_quantity 
            then sls_quantity * ABS(sls_price) 
            else sls_sales
    end as sls_sales,
    sls_quantity,
    case 
        when sls_price is null or sls_price <= 0
            then sls_sales/NULLIF(sls_quantity,0)
            else sls_price
    end as sls_price
from bronze.crm_sales_details


--checking table silve.crm_sales_details

SELECT * from silver.crm_sales_details

-- successfully intergrated

--quality check 
SELECT * from silver.crm_sales_details
--where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
where sls_sales != sls_price * sls_quantity
--where sls_sales <= 0 or sls_quantity<= 0 or sls_price <=0 

----done handling crm_sale_details----------


----now dealing with erp_cust_az12-----


select * from bronze.erp_cust_az12

--data quality checks
SELECT distinct 
    gen,
    case 
        when UPPER(trim(gen)) IN ('M','MALE') then 'Male'
        when UPPER(trim(gen)) IN ('F','FEMALE') then 'Female'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12

--making neccessary transformation 
SELECT
    substring(cid,4,length(cid)) as cid,
    case
        when bdate > CURRENT_DATE then null
        else bdate
    end as bdate,
    case 
        when UPPER(trim(gen)) IN ('M','MALE') then 'Male'
        when UPPER(trim(gen)) IN ('F','FEMALE') then 'Female'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12


-- DDL
DROP TABLE IF EXISTS silver.erp_cust_az12; --ensure if table exist or not
CREATE TABLE silver.erp_cust_az12(
    cid VARCHAR(50),
    bdate Date,
    gen VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


--inserting transform data in silver layer in erp_cust_az12 table
truncate table silver.erp_cust_az12
insert into silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT
    substring(cid,4,length(cid)) as cid,
    case
        when bdate > CURRENT_DATE then null
        else bdate
    end as bdate,
    case 
        when UPPER(trim(gen)) IN ('M','MALE') then 'Male'
        when UPPER(trim(gen)) IN ('F','FEMALE') then 'Female'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12

--checking data
select * from silver.erp_cust_az12
select distinct gen from silver.erp_cust_az12

select bdate from silver.erp_cust_az12
where bdate > CURRENT_DATE
--sucessfully inserted

--now dealing with erp_loc_a101 and transforming


select
    replace(cid,'-','') as cid,
    case 
        when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or trim(cntry) is null then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101

--checking the data quality
SELECT distinct 
    cntry,
    case 
        when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or trim(cntry) is null then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101


--inserting data into silver layer
truncate table silver.erp_loc_a101
insert into silver.erp_loc_a101(cid,cntry)
select
    replace(cid,'-','') as cid,
    case 
        when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or trim(cntry) is null then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101

--succesfully inserted
-- data quality check after insert
select * from silver.erp_loc_a101
where cid is null
select distinct cntry from silver.erp_loc_a101
-- succesfully loaded in silver layer


--now dealing with the rep_px_cat_g1v2
--checking data 
select * FROM bronze.erp_px_cat_g1v2

select * FROM bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

select distinct maintenance FROM bronze.erp_px_cat_g1v2

select distinct cat FROM bronze.erp_px_cat_g1v2

select distinct subcat FROM bronze.erp_px_cat_g1v2

--after checking the data. the data is of good quality and no need to change. now we will insert the data in silver layer.COMMENT
--insert data 
truncate table silver.erp_px_cat_g1v2
insert into silver.erp_px_cat_g1v2 (id, cat,subcat,maintenance)
select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2
--successfully inserted 

--checking inserted data
select * from silver.erp_px_cat_g1v2

select * FROM silver.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

select distinct maintenance FROM silver.erp_px_cat_g1v2

select distinct cat FROM silver.erp_px_cat_g1v2

select distinct subcat FROM silver.erp_px_cat_g1v2

--successfully loaded into silver layer and also checked the data quality

