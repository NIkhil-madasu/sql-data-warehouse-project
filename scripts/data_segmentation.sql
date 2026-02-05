----------------------------------------DATA SEGMENTTATION----------------------------------------------------------------

------GROUP THE DATA BASED ON SPECIFIC RANGE CONDITION HELPS TO UNDERSTAND CORRELATION B/W TWO MEASURES. IT CONVERTS MEASURE INTO DIMENSION--------

----SYNTAX = MEASURE BY MEASURE
----EXMAMPLE: TOTAL PRODUCTS BY SALES, TOTAL CUSTOMERS BY AGE

===================SEGMENT PRODUCTS INTO price RANGE AND COUNT HOW MANY PRODUCTS FALL INTO EACH CATEGORY (USING CTE)======================
with product_segment as
(
SELECT
ProductID,
Product,
Price,
case when Price>=25 then 'high cost'
     when Price<25 then 'low cost'
End Price_Range
FROM Sales.Products
)
select
Price_Range,
COUNT(productid) as Total_products
from product_segment
group by Price_Range
order by Total_products
