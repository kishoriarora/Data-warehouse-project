/*
==========================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
==========================================================
Script Purpose:
   This stored procedure loads data into the 'bronze' schema from external CSV files.
   It performs the following actions:
   - Truncates the bronze table before loading data
   - Uses the 'BULK INSERT' command to load the data form csv files to bronze tables

Parameters : 
  None 
  This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;
=============================================================

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
      DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
      BEGIN TRY
	    SET @batch_start_time = GETDATE();
        print '=================================================================';
		print 'loading Bronze layer';
		print '=================================================================';

		print '----------------------------------------------';
		print 'Loading CRM tables';
		print '-----------------------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		print '>> Inserting Data Into : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		print '>> Truncating Table : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		print '>> Truncating Table : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		print '>> Inserting Table Into : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		print '>> Inserting Table : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		print '>> Truncating Table : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\HP\Desktop\dataaa\databricks_bootcamp_2026-main\datasets\engineering\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>Load Duration = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-----------------------';

		SET @batch_end_time = GETDATE();
		print '-------------------------------'
		print '>>Loading Bronze Layer is Cmpleterd';
		print '   - Total Load Duration : ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '-----------------------------------'
		END TRY
		BEGIN CATCH
		      print '========================================'
			  print 'Error Occured during loading bronze layer'
			  print 'Erroe message' + ERROR_MESSAGE();
			  print 'Erroe message' + CAST (ERrOR_NUMBER() AS NVARCHAR);
			  print 'Erroe message' + CAST (ERROR_STATE() AS NVARCHAR);
			  print '========================================'
		END CATCH
END
