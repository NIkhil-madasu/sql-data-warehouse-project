/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from datasets 
    It performs the following action:
    - Truncates the bronze tables before loading data.

Parameters:
    None.

    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/



create or alter procedure bronze.load_bronze as
begin
   declare @start_time DATETIME, @end_time DATETIME;
   begin try
		  print '=======================================================';
		  print 'LOADING BRONZE LAYER';
		  print '=======================================================';

		  PRINT '-------------------------------------------------------';
		  PRINT 'LOADING SOURCE_A TABLES';
		  PRINT '-------------------------------------------------------';
	    
		  SET @start_time=GETDATE();
		  PRINT '>>TRUNCATING TABLE :bronze.A_cust_info';
		  TRUNCATE TABLE bronze.A_cust_info;

		PRINT'>>INSERTING DATA INTO:bronze.A_cust_info';
		INSERT INTO bronze.A_cust_info values(1,'AW0001','NIKHIL','MADASU','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(2,'AW0002','GNAN','DEEP','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(3,'AW0003','JOHN','BRAINLY','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(4,'AW0004','CHAI','JOSEPH','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(5,'AW0005','RAMANA','MURTHY','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(6,'AW0006','ANU','RADHA','M','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(7,'AW0007','SAI','VENKAT','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(8,'AW0008','JESSIE','CHRIST','S','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(9,'AW0009','DHANU','SRI','S','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(10,'AW00010','JOHN','YANG','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(11,'AW00011','EUGENE','HURANG','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(12,'AW00012','RUBAN','TORESS','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(13,'AW00013','ZHU','CHRISTY','S','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(14,'AW00014','ELIZBETH','JOHNSON','S','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(15,'AW00015','JULIO','ALX','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(16,'AW00016','STARCHE','WEDLY','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(17,'AW00017','MIA','THOMAS','M','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(18,'AW00018','JAMES','MOORE','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(19,'AW00019','VIRAT','KOHLI','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(20,'AW00020','SAMUEL','GREEN','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(21,'AW00021','ALEXANDER','WALKER','S','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(22,'AW00022','DAVID','HILL','M','M','2025-10-6')
		INSERT INTO bronze.A_cust_info values(23,'AW00023','LILY','LOPEZ','S','F','2025-10-6')
		INSERT INTO bronze.A_cust_info values(24,'AW00024','DANIEL','SEKHER','S','MALE','2026-10-6')
		INSERT INTO bronze.A_cust_info values(25,'AW00025','ISABELLA','TAYLOR','M','F','2026-10-6')
		INSERT INTO bronze.A_cust_info values(26,'AW00026','WIILIAM','MILLER','S','F','2026-10-6')
		INSERT INTO bronze.A_cust_info values(27,'AW00027','OLIVIA','DAVIS','M','F','2026-10-6')
		INSERT INTO bronze.A_cust_info values(28,'AW00028','MATHEW','HARRIES','M','M','2026-10-6')
		INSERT INTO bronze.A_cust_info values(29,'AW00029','AMELIA','MARTIN','S','FEM','2026-10-6')
		INSERT INTO bronze.A_cust_info values(30,'AW00030','ETHAN','THOMPSON','M','M','2026-10-6')
		;
		SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';

		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE :bronze.A_prd_info';
		Truncate table bronze.A_prd_info;

		PRINT '>>INSERTING DATA INTO:bronze.A_prd_info';
		BULK INSERT bronze.A_prd_info
		FROM'C:\Users\Lenovo\OneDrive\Documents\sql-data-warehouse-project\datasets\source_A\prd_info.csv'
		WITH(
			 Firstrow=2,
			 FieldTerminator= ',',
			 Tablock
			 );
		SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';
    
	    SET @start_time=GETDATE();

		PRINT '>>TRUNCATING TABLE : bronze.A_sls_details';
		Truncate table bronze.A_sls_details;

		PRINT '>>INSERTING DATA INTO :bronze.A_sls_details';
		BULK INSERT bronze.A_sls_details
		FROM'C:\Users\Lenovo\OneDrive\Documents\sql-data-warehouse-project\datasets\source_A\sls_details.csv'
		WITH(
			 Firstrow=2,
			 FieldTerminator= ',',
			 Tablock
			 );
        	SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';

			 PRINT '-------------------------------------------------------';
			 PRINT 'LOADING SOURCE_B TABLES';
			 PRINT '-------------------------------------------------------';

        SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE : bronze.B_loc_details';
		Truncate table bronze.B_loc_details;

		PRINT '>>INSERTING DATA INTO : bronze.B_loc_details';
		BULK INSERT bronze.B_loc_details
		FROM'C:\Users\Lenovo\OneDrive\Documents\sql-data-warehouse-project\datasets\source_B\loc_details.csv'
		WITH(
			 Firstrow=2,
			 FieldTerminator= ',',
			 Tablock
			 );
        	SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';

		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE : bronze.B_demographics';
		Truncate table bronze.B_demographics;

		PRINT '>>INSERTING DATA INTO : bronze.B_demographics';
		BULK INSERT bronze.B_demographics
		FROM'C:\Users\Lenovo\OneDrive\Documents\sql-data-warehouse-project\datasets\source_B\demographics.csv'
		WITH(
			 Firstrow=2,
			 FieldTerminator= ',',
			 Tablock
			 );
	   SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';
    
		PRINT '>>TRUNCATING TABLE : bronze.B_cat_maintenance';
		Truncate table bronze.B_cat_maintenance;
	    
		SET @start_time=GETDATE();
		PRINT '>> INSERTING DATA INTO : bronze.B_cat_maintenance';
		BULK INSERT bronze.B_cat_maintenance
		FROM'C:\Users\Lenovo\OneDrive\Documents\sql-data-warehouse-project\datasets\source_B\cat_maintenance.csv'
		WITH(
			 Firstrow=2,
			 FieldTerminator= ',',
			 Tablock
		);
		SET @end_time=GETDATE();
		PRINT'>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' SECONDS';
		PRINT'>>-------------------';
	end try
	begin catch
	    PRINT '=============================================='
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'Error Message ' + ERROR_MESSAGE();
    PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '=============================================='
	end catch
 
end
