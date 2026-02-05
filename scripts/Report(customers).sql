/*
==========================================================================================================================================
CUSTOMER REPORT
==========================================================================================================================================
PURPOSE:
      - THIS REPORT CONSOLIDATES KEY CUSTOMER METRICS AND BEHVIOURS

HIGHLIGHTS:
     1 .  GATHERS ESSENTIAL FIELDS SUCH AS NAMES ,AGES, TRANSACTION DETAILS.
	 2 .  SEGMENT CUST INTO CATEGORIES(VIP,REGULAR,NEW).
	 3 .  AGG CUST-LEVEL METRICS:
	      -TOTAL ORDERS
		  -TOTAL SALES
		  -TOTAL QUANTITY
		  -TOTAL PRODUCTS
		  -LIFESPAN(IN MONTHS)
=============================================================================================================================================
*/

------ 1-BASE QUEARY : RETRIVE CORE COLOUMNS FROM TABLES-----------------------
CREATE VIEW  SALES.REPORT_CUSTOMERS AS 
with base_query as
(
SELECT
O.OrderID,
O.ProductID,
O.OrderDate,
O.Sales,
O.Quantity,
c.customerid,
CONCAT(C.FIRSTNAME ,' ',C.LASTNAME) AS Name
FROM SALES.Orders AS O
LEFT JOIN SALES.Customers AS C
ON O.CustomerID=C.CustomerID
)
---------- 2 .  SEGMENT CUST INTO CATEGORIES(VIP,REGULAR,NEW) 
,customer_agg as
(
--------- 3 . TOTAL SALES,TOTAL QUANTITY,tOTAL PRODUCTS, lifespan
select
customerid,
Name,
COUNT(distinct OrderID)as Total_orders,
sum(sales) as Total_sales,
sum(quantity) as total_quantity,
count(distinct ProductID) as total_products,
max(orderdate) as  last_order,
DATEDIFF(month, min(orderdate),max(orderdate)) as Lifespan
from base_query
group by customerid,
Name
)
select
customerid,
Name,
Total_orders,
Total_sales,
total_quantity,
total_products,
last_order,
Lifespan,
case when Lifespan=2 and Total_sales>=100 then 'VIP'
     when Lifespan<2 and Total_sales<100 then 'REGULAR'
	 ELSE 'NEW'
END AS CUSTOMER_SEGMENT
from customer_agg
