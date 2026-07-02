/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.

    Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


IF OBJECT_ID('bronze.A_cust_info' ,'U') IS NOT NULL
    DROP TABLE bronze.A_cust_info;
create table bronze.A_cust_info (
	cst_id INT,
	cst_Key NVARCHAR(50),
	cst_Firstname NVARCHAR(50),
	cst_Lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_Gender NVARCHAR(50),
	cst_Create_date DATE
);
IF OBJECT_ID('bronze.A_prd_info' ,'U') IS NOT NULL
    DROP TABLE bronze.A_prd_info;
create table bronze.A_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_num  NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATETIME,
	prd_end_date DATETIME,
);
IF OBJECT_ID('bronze.A_sls_details' ,'U') IS NOT NULL
    DROP TABLE bronze.A_sls_details;
create table bronze.A_sls_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cst_id INT,
	sls_ord_date INT,
	sls_ship_date INT,
	sls_due_date INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
IF OBJECT_ID('bronze.B_loc_details' ,'U') IS NOT NULL
    DROP TABLE bronze.B_loc_details;
create table bronze.B_loc_details (
	cst_id NVARCHAR(50),
	country NVARCHAR(50)
);
IF OBJECT_ID('bronze.B_demographics' ,'U') IS NOT NULL
    DROP TABLE bronze.B_demographics ;
create table bronze.B_demographics (
	cst_id NVARCHAR(50),
	bdate DATE,
	gender NVARCHAR(50)
);
IF OBJECT_ID('bronze.B_cat_maintenance' ,'U') IS NOT NULL
    DROP TABLE bronze.B_cat_maintenance  ;
create table bronze.B_cat_maintenance (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subct NVARCHAR(50),
	maintenance NVARCHAR(50)
);
