IF OBJECT_ID('silver.cust_info' ,'U') IS NOT NULL
    DROP TABLE silver.cust_info;
CREATE TABLE silver.cust_info (
	cst_id INT,
	cst_Key NVARCHAR(50),
	cst_Firstname NVARCHAR(50),
	cst_Lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_Gender NVARCHAR(50),
	cst_Create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.prd_info' ,'U') IS NOT NULL
    DROP TABLE silver.prd_info;
CREATE TABLE silver.prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_num  NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATE,
	prd_end_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.sls_details' ,'U') IS NOT NULL
    DROP TABLE silver.sls_details;
CREATE TABLE silver.sls_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cst_id INT,
	sls_ord_date DATE,
	sls_ship_date DATE,
	sls_due_date DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.demographics' ,'U') IS NOT NULL
    DROP TABLE silver.demographics;
CREATE TABLE silver.demographics (
	cst_id NVARCHAR(50),
	bdate DATE,
	gender NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.loc_details' ,'U') IS NOT NULL
    DROP TABLE silver.loc_details;
CREATE TABLE silver.loc_details (
	cst_id NVARCHAR(50),
	country NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.cat_maintenance' ,'U') IS NOT NULL
    DROP TABLE silver.cat_maintenance;
CREATE TABLE silver.cat_maintenance (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
