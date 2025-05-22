/*
STRORED PROCEDURE: LOAD BRONZE  LYER (SOURCE-> BRONZE)
SCRRIPT PURPOSE: THIS SP LOADS DATA INTO BRONZE SCHEMA  FROM EXTERNAL CSV FILES
IT PERFORMANCES THE BELOW ACTIONS:
      TRUNCTE THE BRONZE TABLES BEFORE LODING THE DATA
       USED THE BULK INSERT COMMAND TO LOAD THE DATA FROM CSV FILES TO BRONZE TABLES
*/

----------------------------------------------SILVER LAYER---------------------------------------------------------------------------------------
EXECUTE silver.load_silver
CREATE OR  ALTER PROCEDURE silver.load_silver AS
BEGIN
     DECLARE @start_time DATETIME,@end_time DATETIME;
BEGIN TRY
   PRINT '===================';
   PRINT'Loading Silver LAYER ';
   PRINT '===================';

--TRANSFORMED DATA OF CRM_CUST_INFO INSERTED INTO SILVER TABLE(CRM_CUST_INFO)
SET @start_time=GETDATE();
PRINT'>> TRUNCATING TABLE: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info
(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_gndr,
cst_matrerial_staus,
cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_matrerial_staus))='F' THEN 'Female'
     WHEN UPPER(TRIM(cst_matrerial_staus))='M' THEN 'Male'
	 ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
  SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
 FROM bronze.crm_cust_info
 )t wHERE flag_last=1 ;

 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

 --TRANSFORMED DATA FOR SILVER LAYER FOR CRM_PRD_INFO
 SET @start_time=GETDATE();
 PRINT'>> TRUNCATING TABLE: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info 
(
prd_id,
prd_key,
cat_id,
prd_nm,
prd_cost,
prd_line ,
prd_start_dt ,
prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
     WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
     ELSE 'n\a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

--TRANSFORMED DATA FOR SILVER LAYER FOR CRM_SALES_DETAILS
SET @start_time=GETDATE();
PRINT'>> TRUNCATING TABLE: silver.crm_sales_detailS';
TRUNCATE TABLE silver.crm_sales_detailS;
PRINT '>> INSERTING DATA INTO: silver.crm_sales_detailS';
INSERT INTO silver.crm_sales_detailS
(
 sls_ord_num,
 sls_prd_key,
 sls_cust_id,
 sls_order_dt,
 sls_ship_dt ,
 sls_due_dt,
 sls_sales,
 sals_quantity,
 sls_price 
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
END AS sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales<0 OR sls_sales!=sals_quantity*ABS(sls_price)
        THEN sals_quantity*ABS(sls_price)
		ELSE sls_sales
END AS sls_sales,

sals_quantity,

CASE WHEN sls_price IS NULL OR sls_price<0 
        THEN sls_sales / NULLIF(sals_quantity,0)
		ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_detailS;

SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';


--TRANFORMATION OF DATA SILVER LAYER ERP_CUST_AZ12
 SET @start_time=GETDATE();
PRINT'>> TRUNCATING TABLE: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12
(
 cid,
 bdate,
 gen
 )
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
    ELSE cid
END AS cid,
CASE WHEN bdate>GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','Femle') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	  ELSE 'N\A'
END gen
FROM bronze.erp_cust_az12;
SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

 --TRANSFORMATION OF DTA FOR ERP_LOC_A101
 SET @start_time=GETDATE();
PRINT'>> TRUNCATING TABLE: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101
(
cid,
cntry
)
SELECT
REPLACE(cid, '-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
     WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)= ''OR cntry IS NULL THEN 'N\A'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;
SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

--TRANSFORMATION AND DAT QUALITY IN SILVER.ERP_PX_G1V2

--CHECK FOR UN WANTED SPACES FOR CAT
-----SELECT *FROM bronze.erp_px_g1v2
------WHERE cat!=TRIM(cat)
--NO UNWNTED SPACES IN CAT

--CHECK FOR UN WANTED SPACES FOR SUBCAT
------SELECT *FROM bronze.erp_px_g1v2
------WHERE cat!=TRIM(subcat)
--NO UNWNTED SPACES IN SUBCAT

--CHECK FOR UN WANTED SPACES FOR Manintance
-----SELECT *FROM bronze.erp_px_g1v2
-----WHERE cat!=TRIM(maintenance)
-----NO UNWNTED SPACES IN MAINTANANCE

--THERE IS NO DATA QUALITY ISUUSE IN THIS EXP_PX_G1V2
SET @start_time=GETDATE();
PRINT'>> TRUNCATING TABLE: silver.erp_px_g1v2';
TRUNCATE TABLE silver.erp_px_g1v2;
PRINT '>> INSERTING DATA INTO: silver.erp_px_g1v2';
INSERT INTO silver.erp_px_g1v2
(
 id,
 cat,
 subcat,
 maintenance
 )
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_g1v2
SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
     
	 END TRY
	BEGIN CATCH
	     PRINT'========================================'
		 PRINT'ERROR OCCURED DURING LOADING SILVER LAYER'
		 PRINT 'Error Message' + ERROR_MESSAGE();
		 PRINT 'Error Message' + CAST( ERROR_NUMBER()AS NVARCHAR);
		 PRINT 'Error Message' + CAST( ERROR_STATE()AS NVARCHAR);
		 PRINT'========================================'
	END CATCH

END
