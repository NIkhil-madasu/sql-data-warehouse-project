--------------------------------------------CHANGE OVER TIME(TRENDS)-------------------------------------------------

--------ANALYZE HOW MEASURE EVOLVES OVER TIME ,HELPS TRACK TRENDS AND IDENTIFY SEASONALITY IN YOUR DATA -------------------------------

-----SYNTAX = AGG[MEASURE] BY [ DATE DIMENSION]
-----EXMPLE: TOTAL SALES BY YEAR , AVG COST BY MONTH

=================================ANALYZE SALES PERFORMANCE OVER TIME================================================================

------ANALYZE SALES PERFORMNCE OVER TIME --------------
 ------WE ARE CALCULATING SALES BY YEAR AND MONTH-----------------
 SELECT
 YEAR(Order_Date) AS ORDER_YEAR,
 MONTH(Order_Date) AS ORDER_MONTH,
 SUM(Sales_amount) AS TOTAL_SALES
 FROM gold.fact_sales
 where Order_Date is not null
 GROUP BY YEAR(Order_Date),MONTH(Order_Date)
 ORDER BY YEAR(Order_Date),MONTH(Order_Date) DESC

 ---------ANALYZE AVG PRICE OVER TIME---------------------
  ------WE ARE CALCULATING PRICES BY YEAR AND MONTH--------
  SELECT
 YEAR(Order_Date) AS ORDER_YEAR,
 MONTH(Order_Date) AS ORDER_MONTH,
 AVG(Price) AS AVG_PRICE
 FROM gold.fact_sales
 where Order_Date is not null
 GROUP BY YEAR(Order_Date),MONTH(Order_Date)
 ORDER BY YEAR(Order_Date),MONTH(Order_Date) DESC
