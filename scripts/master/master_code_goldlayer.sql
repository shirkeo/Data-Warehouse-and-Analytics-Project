--gold layer
--creating first object
--join the rables as per the intergration model and business logic
-- also checking if we are creating any duplicates or not
select cst_id, count(*) from(
    select  ci.cst_id,
            ci.cst_key,
            ci.cst_firstname,
            ci.cst_lastname,
            ci.cst_marital_status,
            ci.cst_gndr,
            ci.cst_create_date,
            ca.bdate,
            ca.gen,
            la.cntry
    from silver.crm_cust_info ci
    left JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la on ci.cst_key = la.cid) t GROUP BY cst_id
    HAVING count(*)>1

-- after checking there were no duplicates and moving forward with data integration issue with gender columns in table.
    select  ci.cst_id,
            ci.cst_key,
            ci.cst_firstname,
            ci.cst_lastname,
            ci.cst_marital_status,
            ci.cst_gndr,
            ci.cst_create_date,
            ca.bdate,
            ca.gen,
            la.cntry
    from silver.crm_cust_info ci
    left JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la on ci.cst_key = la.cid

--dealing with gender columns
select distinct
    ci.cst_gndr,
    ca.gen,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr   -----CRM is the master for gender info
        else COALESCE(ca.gen,'n/a')
    end as new_gen

from silver.crm_cust_info ci
left JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la on ci.cst_key = la.cid
ORDER BY ci.cst_gndr,ca.gen

--- made changes to master code and also comfortable attribute names
DROP VIEW IF EXISTS gold.dim_customer;
create view gold.dim_customer AS
select ROW_NUMBER() over (order by cst_id) AS customer_key, 
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr   -----CRM is the master for gender info
        else COALESCE(ca.gen,'n/a')
    end as gender,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date      
from silver.crm_cust_info ci
left JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la on ci.cst_key = la.cid

--quality check of view

select * from gold.dim_customer
select distinct gender from gold.dim_customer



-- creating second object 
--checking for duplicates keys.
select prd_key,count(*) from(
select pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is Null) t   --- filter out all historical data
group by prd_key
having count(*)>1

--no such Duplicates
--sorting the columns and giving friendly names
drop view IF EXISTS gold.dim_products;
create view gold.dim_products as
select 
    ROW_NUMBER() over (ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is Null
--object created

select * from gold.dim_products 

--now create fact table
drop view IF EXISTS gold.fact_sales;
create view gold.fact_sales as
select sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id

--select * from gold.dim_customer c
select * from gold.fact_sales f
left join gold.dim_customer c
on c.customer_key = f.customer_key
where c.customer_key is Null
