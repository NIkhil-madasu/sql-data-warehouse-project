/*
DDL SCRIPT: CREATE BRONZE TABLES FROM CRM AND ERP FOLDERS
SCRIPT PURPOSE: THIS SCRIPT CREATE A TABLES IN BRONZE LAYER. RUN THIS SCRIPT TO REDEFIENE THE DDL SCTRUCRE OF BRONZE TABLES
*/

CREATE TABLE bronze.crm_cust_info
(
  cst_id INT,
  cst_key NVarchar(50),
  cst_firstname NVarchar(50),
  cst_lastname NVarchar(50),
  cst_matrerial_staus NVarchar(50),
  cst_gndr NVarchar(50),
  cst_create_date date
 );

CREATE TABLE bronze.crm_prd_info
(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

CREATE TABLE bronze.crm_sales_detailS
(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sals_quantity INT,
sls_price INT,
);

CREATE TABLE bronze.exp_loc_a101
(
 cid   NVARCHAR(50),
 cntry  NVARCHAR(50)
 );

 CREATE TABLe bronze.exp_cust_az12
 (
  cid NVARCHAR(50),
  bdate DATE,
  gen NVARCHAR(50)
  );

  CREATE TABLE bronze.exp_px_g1v2
  (
   id VARCHAR(50),
   cat VARCHAR(50),
   subcat VARCHAR(50),
   maintenance VARCHAR(50)
   );
