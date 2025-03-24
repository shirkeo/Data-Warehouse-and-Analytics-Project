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

