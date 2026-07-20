---------------------------------------------------MEASURE EXPLORATION------------------------------------------------------------

--CALCULATE HIGHEST AND LOWEST LEVEL OF AGGREGATION USING SUM,AVG,COUNT E.T.C---------------------

--SYNTAX = AGG[MEASURE]--
--EXAMPLE: SUM(SALES),AVG(PRICE),SUM(QUANTITY),COUNT(SALES)---

-------FIND THE TOTAL SALES-------
select sum(sales_amount)as total_sales from gold.fact_sales

-------FIND HOW MANY ITEMS ARE SOLD------
select sum(Quantity)as total_items from gold.fact_sales

--------FIND AVG SELLING PRICE-----------
select avg(price) as avg_pice from gold.fact_sales

--------FIND THE TOTAL NUMBER OF ORDERS----------
select count(distinct Order_Number) as total_orders from gold.fact_sales

--------FIND THE TOTAL NUMBER OF PRODUCTS----------
select count(distinct product_key) as total_products from gold.dim_Products

--------FIND THE TOTAL NUMBER OF CUSTOMERS----------
select count(distinct Customer_Key) as total_customers from gold.dim_Customers

--------FIND THE TOTAL NUMBER OF CUSTOMERS THAT PLACED ORDER----------
select count(distinct Customer_Key) as total_customers from gold.fact_sales

--------GENERATE A REPORT TO SHO ALL KEY MATRICS OF OUR BUSSINESS------------
select 'total_sales' as Measure_name ,sum(sales_amount)as Measure_value from gold.fact_sales
union all
select'total_items' as Measure_name, sum(Quantity)as Measure_value from gold.fact_sales
union all
select 'avg_pice',avg(price)  from gold.fact_sales
union all
select'total_orders', count(distinct Order_Number)  from gold.fact_sales
union all
select 'total_products',count(distinct product_key)  from gold.dim_Products
union all
select'total_customers', count(distinct Customer_Key)  from gold.dim_Customers
union all
select'total_customers', count(distinct Customer_Key)  from gold.fact_sales
