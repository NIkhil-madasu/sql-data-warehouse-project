/*
WE CREATED A VIEW FOR GOLD LAYERS 2 DIM AND 1 FACT DIM(CUSTOMERS,PRODUCTS) FACT(SALES)
*/

-- VIEW FOR  GOLD LAYER FOR DIMENSION CUSTOMERS
CREATE VIEW gold.dim_Customers AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS CUSTOMER_KEY,
ci.cst_id AS CUSTOMER_ID,
ci.cst_key AS CUSTOMER_NUM,
ci.cst_firstname AS First_Name,
ci.cst_lastname AS Lat_Name,
ci.cst_matrerial_staus AS  Marital_Status,
ci.cst_gndr AS Gender
FROM silver.crm_cust_info AS ci

--  VIEW FOR GOLD  LAYER FOR DIMENSION PRODUCTS
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER()OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS Product_Key,
pn.prd_id AS Product_Id,
pn.prd_key AS Product_Num,
pn.prd_nm AS Product_Name,
pn.cat_id AS Categorey_Id,
pn.prd_cost AS Cost,
pn.prd_line AS Product_Line,
pn.prd_start_dt AS Start_Date
FROM silver.crm_prd_info AS pn
--WE USED WHERE COND BELOW BECAUSE WE WANT ONLY LATEST DATE INFO IN THE PRD_END_DT
--IF WE GET NULL IN PRD_END_DT THEN IT IS LASTEST DATE DATA
WHERE prd_end_dt IS NULL

--  VIEW FOR GOLD  LAYER FOR FACT SALES
 CREATE VIEW gold.fact_sales AS
SELECT
--WE CAN'T SUGGARGATE BY ROW_NUM BECAUSE IT IS FCT ONLY DIM ALLOWS SUGGARGATE
sd.sls_ord_num AS Order_Number,
--THIS BELOW PRODUCT_KEY CAME FROM DIM PRODUTCS FROM GOLD LAYER BECAUSE IT IS SURROGTE KEYS
pr.Product_Key,
--THIS BELOW CUSTOMER_KEY CAME FROM DIM CUSTOMERS FROM GOLD LAYER BECAUSE IT IS SURROGTE KEYS
cs.CUSTOMER_KEY,
sd.sls_order_dt AS Order_Date,
sd.sls_ship_dt AS Shipping_Date,
sd.sls_due_dt AS Due_Date,
sd.sls_sales AS Sales_Amount,
sd.sals_quantity AS quantity,
sd.sls_price AS Price
FROM silver.crm_sales_detailS AS sd
--WE JOIN GOLD INSTED OF SILVER LAYER BECAUSE  WE GET ONLY SUGGARGATE KEY FROM GOLD LAYER ONLY
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key=pr.Product_Num
LEFT JOIN gold.dim_Customers AS cs
ON sd.sls_cust_id=cs.CUSTOMER_ID




