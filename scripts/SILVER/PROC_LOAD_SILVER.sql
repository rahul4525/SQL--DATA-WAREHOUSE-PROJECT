/*
--================================================================
STORED PROCEDURE :LOAD SILVER LAYER (BRONZE->>  SILVER )
--================================================================
 SCRIPT PERFORM :
                 THIS STORED PROCEDURE PERFORM THE ETL PROCESS(EXTRACT,  TRANSFORM AND LOAD ) PROCESS TO POPULATE THE SILVER  SCHEMA
                FROM THE BRONZE SCHEMA

ACTION  PERFORM :
-- TRUNCATE SILVER TABLE 
-- INSERT TRANSFORM AND CLEANSED DATA  FROM BRONZE LAYER INTO SILVER LAYER 

PARAMETER:
NONE  
THIS STORED PROCEDURE DOES NOT ACCEPT  ANY PARAMETER OR  RETURN  AN  VALUE 
--

USAGE EXAMPLE :
EXEC  SILVER.LOAD_SILVER

*/



CREATE  OR ALTER PROCEDURE SILVER.LOAD_SILVER
AS
BEGIN
DECLARE @START_TIME DATETIME, @END_TIME DATETIME , @BATCH_START_TIME  DATETIME, @BATCH_END_TIME DATETIME;
 
 BEGIN TRY 
 SET @BATCH_START_TIME= GETDATE();
 PRINT '====================================='
 PRINT 'LOADING SILVER TABLE '
  PRINT '====================================='

  PRINT '====================================='
 PRINT 'LOADING CRM TABLE '
  PRINT '====================================='
	--===============================================================================================
	PRINT '>>TRUNCATE TABLE :silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>>INSERT DATA INTO  :silver.crm_cust_info'
	SET @START_TIME=GETDATE();
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname)  AS cst_lastname,

		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,

		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,

		cst_create_date
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (
				PARTITION BY cst_id
				ORDER BY cst_create_date DESC
			) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) t
	WHERE flag_last = 1;
	SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'

	--===============================================================================================
	SET @START_TIME=GETDATE();
	PRINT '>>TRUNCATE TABLE :SILVER.CRM_PRD_INFO'
	TRUNCATE TABLE SILVER.CRM_PRD_INFO
	PRINT '>>INSERT DATA INTO  :SILVER.CRM_PRD_INFO'
	INSERT INTO SILVER.CRM_PRD_INFO(
	prd_id, 
	CAT_ID ,  
	prd_key ,
	prd_nm   ,
	prd_cost ,
	prd_line   ,
	prd_start_dt  ,
	prd_end_dt 
	)
	-- CREATE CLEAN SILVER TABLE 

	  SELECT  prd_id,
	  REPLACE(SUBSTRING(prd_key,1 ,5), '-','_') AS CAT_ID,
	  SUBSTRING(prd_key,7 ,LEN(PRD_KEY)) AS PRD_KEY,
	  prd_nm,
	  ISNULL( PRD_COST,0) AS PRD_COST,  -- HANDLE NULL VALUES WITH 0 WE CAN  ALSO USE COALESCE INSTED OF IS NULL BECAUSE IS NULL IS THE SQL SERVER FUNCTION BUT COALESCE WORKS ON EACH OF US RDBMS IT ALSO CONTAIN MULTIPLE ARGUMNET 
	  CASE --  DATA NORMALIZATION - CONVERT THE CODED DATA INTO A WELL REDABLE FORMAT
		  WHEN TRIM(prd_line)= 'M' THEN 'MOUNTAIN'
		  WHEN TRIM(prd_line)= 'R' THEN 'ROAD'  
		  WHEN TRIM(prd_line)= 'S' THEN 'SOLO'
		  WHEN TRIM(prd_line)= 'T' THEN 'TOUR'
		  ELSE 'N/A'
	  END  AS PRD_LINE,
	  CAST(prd_start_dt  AS  DATE) AS PRD_START_DT,
	  CAST(LEAD(prd_START_dt) OVER (PARTITION BY PRD_KEY  ORDER BY PRD_START_DT )-1 AS DATE) AS PRD_END_DT--  DATA ENRICHMENT :  ADD NEW RELEVANT DATA TO ENHANCE THE DATA FOR ANALYSIS
	  FROM BRONZE.CRM_PRD_INFO

	  SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'

	--===============================================================================================
	SET @START_TIME=GETDATE();
	PRINT '>>TRUNCATE TABLE :silver.CRM_SALES_DETAILS'
	TRUNCATE TABLE silver.CRM_SALES_DETAILS
	PRINT '>>INSERT DATA INTO  :silver.CRM_SALES_DETAILS'
  
	INSERT INTO silver.CRM_SALES_DETAILS(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)

	SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt<=0  OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE  CAST(CAST(sls_order_dt  AS VARCHAR)AS DATE)
		END AS sls_order_dt,
	
		CASE WHEN sls_ship_dt<=0  OR LEN(sls_ship_dt)!=8 THEN NULL
		ELSE  CAST(CAST(sls_ship_dt  AS VARCHAR)AS DATE)
		END AS sls_ship_dt, 

		CASE WHEN sls_due_dt<=0  OR LEN(sls_due_dt)!=8 THEN NULL
		ELSE  CAST(CAST(sls_due_dt  AS VARCHAR)AS DATE)
		END AS sls_due_dt,

		CASE WHEN  sls_sales<=0 then 0
		else sls_sales
		end sls_sales,
		sls_quantity,
		sls_price 
	FROM BRONZE.CRM_SALES_DETAILS

	SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'


	--===============================================================================================
	
	
  PRINT '====================================='
 PRINT 'LOADING ERP TABLE '
  PRINT '====================================='

	SET @START_TIME=GETDATE();
	PRINT '>>TRUNCATE TABLE :SILVER.ERP_CUST_AZ12'
	TRUNCATE TABLE SILVER.ERP_CUST_AZ12
	PRINT '>>INSERT DATA INTO  :SILVER.ERP_CUST_AZ12'

	INSERT INTO  SILVER.ERP_CUST_AZ12( CID, BDATE,GEN)
	SELECT 
	--REMOVE NAS PREFIX IF PRESENT
	CASE 
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4, LEN(CID))
		ELSE CID 
	END  AS CID ,
	-- SET FUTURE BIRTHDATE TO NULL
	CASE 
		WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
	END AS BDATE, 
	--NORMALIZE GENDER VALUES HANDLE UNKNOWN CASES
	CASE
		 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS GEN
	 FROM BRONZE.ERP_CUST_AZ12

	 SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'



	 --====================================================================================
SET @START_TIME=GETDATE();
	PRINT '>>TRUNCATE TABLE :SILVER.ERP_LOC_A101'
	TRUNCATE TABLE SILVER.ERP_LOC_A101
	PRINT '>>INSERT DATA INTO  :SILVER.ERP_LOC_A101'

	INSERT  INTO SILVER.ERP_LOC_A101(CID , CNTRY)
	SELECT REPLACE(CID, '-','') AS CID , 
	CASE WHEN trim(CNTRY)  IN ('USA','United States','US') THEN 'United States of America'
	when TRIM(cntry) = 'DE' THEN 'Germany'
	when TRIM(cntry) ='' OR CNTRY IS NULL THEN 'N/A'
	ELSE TRIM(CNTRY)
	END CNTRY 
	FROM BRONZE.ERP_LOC_A101

	SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'


	--=======================================================================================
SET @START_TIME=GETDATE();
	PRINT '>>TRUNCATE TABLE :SILVER.ERP_PX_CAT_G1V2'
	TRUNCATE TABLE SILVER.ERP_PX_CAT_G1V2
	PRINT '>>INSERT DATA INTO  :SILVER.ERP_PX_CAT_G1V2'
	INSERT INTO SILVER.ERP_PX_CAT_G1V2(
	ID,
	CAT ,
	SUBCAT,
	MAINTENANCE
	)
	SELECT ID, 
		CAT ,
		SUBCAT,
		MAINTENANCE
	FROM BRONZE.ERP_PX_CAT_G1V2

	SET @END_TIME =GETDATE();
	PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'

	SET @BATCH_END_TIME=GETDATE()

	PRINT '>> LOAD  BATCH  DURATION' + CAST(DATEDIFF(SECOND,@BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + 'SECONDS'
	PRINT '>>================'

END TRY

BEGIN CATCH
PRINT '====================================================='
PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '====================================================='

END CATCH
	--===============================================================================================
END

EXEC SILVER.LOAD_SILVER
