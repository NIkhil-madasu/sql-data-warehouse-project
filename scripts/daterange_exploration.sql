----------------------------------DATE EXPLORATION----------------------------------------------------------------

----IDENTIFY THE EARLIEST AND OLDEST DATES(BOUNDARIES) UNDERSATND THE SCOPE OF DATA AND SPAN  HERE WE CAN FIND AGES, DATE OF BIRTHS------------

--SYNTAX= MAX/MIN[DATE DIM]
--EXAMPLE: MAX/MIN [ORDER_DATE],MAX/MIN[BIRTH_DATE] ,MAX/MIN[CRATE_DATE]

-------------FIND THE YOUNGEST AND OLDEST CUSTOMESR AND FIND THEIR AGES-------------------
SELECT
Min(Birthdate) as old_customer_bdate,
datediff(year,Min(Birthdate),GETDATE())as  old_customer_age,
Max(Birthdate) as young_customer_bdate,
datediff(year,max(Birthdate),GETDATE())as  young_customer_age
FROM gold.dim_Customers


--FIND THE DATE OF FIRST AND LAST ORDER  AND ALSO FIND HOW MANY YEARS OF SALES ARE AVAILABLE--
SELECT
MIN(Order_Date) AS First_order_date,
MAX(Order_Date) AS Last_order_date,
DATEDIFF(year,MIN(Order_Date),MAX(Order_Date))as Order_range_Year
FROM gold.fact_sales
