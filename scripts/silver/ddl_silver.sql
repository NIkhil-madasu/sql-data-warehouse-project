/*
DDL SCRIPT: CREATE SILVER TABLES FROM CRM AND ERP FOLDERS
SCRIPT PURPOSE: THIS SCRIPT CREATE A TABLES IN SILVER LAYER. RUN THIS SCRIPT TO REDEFIENE THE DDL SCTRUCRE OF SILVER TABLES
*/

--CREATION OF 6 EMPTY TABLES IN DWH DATABASE FOR SILVER LAYER

--CRM FILES FOR SILVER LAYER
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
       DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
  cst_id INT,
  cst_key NVarchar(50),
  cst_firstname NVarchar(50),
  cst_lastname NVarchar(50),
  cst_matrerial_staus NVarchar(50),
  cst_gndr NVarchar(50),
  cst_create_date date,
  dwh_create_date DATETIME2 DEFAULT GETDATE()
 );
 IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
       DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
prd_id INT,
prd_key NVARCHAR(50),
cat_id NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID('silver.crm_sales_detailS','U') IS NOT NULL
       DROP TABLE silver.crm_sales_detailS;
CREATE TABLE silver.crm_sales_detailS
(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sals_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--ERP FILES FOR SILVER LAYER
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
       DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
 cid   NVARCHAR(50),
 cntry  NVARCHAR(50),
 dwh_create_date DATETIME2 DEFAULT GETDATE()
 );
 IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
       DROP TABLE silver.erp_cust_az12;
 CREATE TABLe silver.erp_cust_az12
 (
  cid NVARCHAR(50),
  bdate DATE,
  gen NVARCHAR(50),
  dwh_create_date DATETIME2 DEFAULT GETDATE()
  );
  IF OBJECT_ID('silver.erp_px_g1v2','U') IS NOT NULL
       DROP TABLE silver.erp_px_g1v2;
  CREATE TABLE silver.erp_px_g1v2
  (
   id VARCHAR(50),
   cat VARCHAR(50),
   subcat VARCHAR(50),
   maintenance VARCHAR(50),
   dwh_create_date DATETIME2 DEFAULT GETDATE()
   );
