--Create database - "datawarehouse"
Create database datawarehouse;


-- Step 1: Create Schemas
--------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
