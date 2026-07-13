/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_Customers AS
select
ROW_NUMBER() OVER(ORDER BY c.cst_id) as Customer_Key, -----Suggorgate key created for this customer(dimension)table
c.cst_id as Customer_Id,
c.cst_Key as Customer_Number,
c.cst_Firstname as First_Name,
c.cst_Lastname as Last_Name,
lo.country as Country,
c.cst_marital_status as Marital_Status,
case when c.cst_Gender != 'n\a' then c.cst_Gender
     else coalesce(d.gender,'n\a')
end as Gender,
d.bdate as Birthdate,
cst_Create_date as Create_Date
from silver.cust_info as c
left join silver.demographics as d
on c.cst_Key=d.cst_id
left join silver.loc_details as lo
on c.cst_Key=lo.cst_id

--select * from gold.dim_Customers;

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW Gold.dim_Products as
select
ROW_NUMBER() OVER(ORDER BY p.prd_start_date,P.prd_key)AS Product_Key,-----as it prouct table is dim so we should crate suggorgate primary key
p.prd_id as Product_Id,
p.prd_key as Product_Number,
p.prd_num as Product_Name,
p.cat_id as Category_Id,
cm.cat as Category,
cm.subcat as SubCategory,
cm.maintenance as Maintenance,
p.prd_cost as Cost,
p.prd_line as Product_Line,
p.prd_start_date as Start_Date
from silver.prd_info as p
left join silver.cat_maintenance as cm
on p.cat_id=cm.id
where prd_end_date is null-- FILTERED OUT THE HISTORICL OLD DATA AND END_DATE

--select * from gold.dim_products;

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW  GOLD.fact_sales as
SELECT
    s.sls_ord_num as Order_Number,
    s.sls_prd_key as Product_Key,
    c.Customer_Key,
    s.sls_ord_date as Order_Date,
    s.sls_ship_date as Shipping_Date,
    s.sls_due_date as Due_Date,
    s.sls_sales  as Sales_amount,
    s.sls_quantity as Quantity,
    s.sls_price as Price
FROM silver.sls_details AS s
LEFT JOIN gold.dim_Customers AS c
    ON c.Customer_Id = s.sls_cst_id

-- select * from gold.fact_sales
