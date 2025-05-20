/*
STRORED PROCEDURE: LOAD BRONZE  LYER (SOURCE-> BRONZE)
SCRRIPT PURPOSE: THIS SP LOADS DATA INTO BRONZE SCHEMA  FROM EXTERNAL CSV FILES
IT PERFORMANCES THE BELOW ACTIONS:
      TRUNCTE THE BRONZE TABLES BEFORE LODING THE DATA
       USED THE BULK INSERT COMMAND TO LOAD THE DATA FROM CSV FILES TO BRONZE TABLES
*/

EXEC bronze.load_bronze
--STORED PROCEDURE
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
  DECLARE @start_time DATETIME,@end_time DATETIME;
BEGIN TRY
   PRINT '===================';
   PRINT'Loading Bronze LAYER ';
   PRINT '===================';

   --CRM DATASET FILES
   SET @start_time=GETDATE();
   PRINT'>> TRUNCATING TABLES:bronze.crm_cust_info';
   TRUNCATE TABLE bronze.crm_cust_info;
    
	PRINT'>>INSERTING DATA INTO:bronze.crm_cust_info';
   BULK INSERT bronze.crm_cust_info
   FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
  WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	);
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	 SET @start_time=GETDATE();
	 PRINT'>> TRUNCATING TABLES:bronze.crm_prd_info';
     TRUNCATE TABLE bronze.crm_prd_info;

    PRINT'>>INSERTING DATA INTO:bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	);
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	 SET @start_time=GETDATE();
   PRINT'>> TRUNCATING TABLES:bronze.crm_sales_details';
  TRUNCATE TABLE bronze.crm_sales_details;

  PRINT'>>INSERTING DATA INTO:bronze.crm_sales_details.';
  BULK INSERT bronze.crm_sales_details
  FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
  WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	); 
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	 --ERP DATASET FILES
	  SET @start_time=GETDATE();
   PRINT'>> TRUNCATING TABLES:bronze.erp_cust_z12';
   TRUNCATE TABLE bronze.erp_cust_z12;
    
	PRINT'>>INSERTING DATA INTO:bronze.erp_cust_z12';
   BULK INSERT bronze.erp_cust_z12
   FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
  WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	);
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	  SET @start_time=GETDATE();
   PRINT'>> TRUNCATING TABLES:bronze.erp_loc_a101';
   TRUNCATE TABLE bronze.erp_loc_a101;
    
	PRINT'>>INSERTING DATA INTO:bronze.erp_loc_a101';
   BULK INSERT bronze.erp_loc_a101
   FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
  WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	);
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	  SET @start_time=GETDATE();
   PRINT'>> TRUNCATING TABLES:bronze.erp_px_cat_g1v2';
   TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
	PRINT'>>INSERTING DATA INTO:bronze.erp_px_cat_g1v2';
   BULK INSERT bronze.erp_px_cat_g1v2
   FROM 'C:\Users\Lenovo\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
  WITH (
     FIRSTROW=2,
	 FIELDTERMINATOR=',',
	 TABLOCK
	);
	 SET @end_time=GETDATE();
	 PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';

	END TRY
	BEGIN CATCH
	     PRINT'========================================'
		 PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		 PRINT 'Error Message' + ERROR_MESSAGE();
		 PRINT 'Error Message' + CAST( ERROR_NUMBER()AS NVARCHAR);
		 PRINT 'Error Message' + CAST( ERROR_STATE()AS NVARCHAR);
		 PRINT'========================================'
	END CATCH
END
