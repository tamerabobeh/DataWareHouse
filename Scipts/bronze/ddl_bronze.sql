/*
DDL Script: Create Bronze tables 

scripit purpose 
 this script creates tables in the bronze schema, 
*/
=================================================================

create database datawarehousenew
create database datawarehouse
use datawarehouse
create schema bronze;
go
create schema silver;
go
create schema Gold;

create table Bronze.crm_cust_info
              (
			  cst_id  int,
			  cst_key int,
			  cst_firstname	nvarchar(50),
			  cst_lastname nvarchar(50),	
			  cst_marital_status nvarchar(50),	
			  cst_gndr nvarchar (50),	
			  cst_create_date date 
			  )
			  alter table Bronze.crm_cust_info alter column  cst_key nvarchar(50)
			  select top 10 * from Bronze.crm_cust_info
create table Bronze.crm_prd_info
               (
			  prd_id  int,
			  prd_key int,
			  prd_nm	nvarchar(50),
			  cst_cost int,	
			  cst_line nvarchar(50),	
			  cst_start_dt date,	
			  cst_end_dt date 
			  ) 
			  alter table Bronze.crm_prd_info alter column prd_key nvarchar(50)
create table Bronze.crm_sales_datails
             ( 
			 sls_ord_num nvarchar(50),
			 sls_prd_key nvarchar(50),
		     sls_cust_id int,
			 sls_order_dt int,
			 sls_ship_dt int,
			 sls_due_dt int,
			 sls_sales int,
			 sls_quantity int,
			 sls_price int
			 )

create table bronze.erp_cust_az
            (
			CID nvarchar(50),
			BDATE date,
			GEN nvarchar(50)
			)

create table bronze.erp_loc_A101
            (
			cid nvarchar(50),
			cntry nvarchar(50)
			)
 create table bronze.erp_px_cat_g1v2
           (
		   ID nvarchar(50),
		   CAT nvarchar(50),		      
		   SUBCAT nvarchar(50), 
		   MAINTENANCE nvarchar(50)
		   )

alter table Bronze.crm_prd_info alter COLUMN  cst_end_dt datetime;
create 
or alter procedure bronze.load_bronze as
begin 
DECLARE @START_TIME DATETIME, 
@END_TIME DATETIME, 
@BATCH_START_TIME DATETIME, 
@BATCH_END_TIME DATETIME;
BEGIN TRY 
SET 
  @BATCH_START_TIME = GETDATE();
PRINT '====================================';
PRINT 'LOADING BRONZE LAYER ';
PRINT '====================================';
PRINT ' LOADING CRM TABLES' PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE Bronze.crm_cust_info ';
SET 
  @START_TIME = GETDATE() truncate table Bronze.crm_cust_info;
PRINT '>>INSERTING DATA INTO Bronze.crm_cust_info';
bulk insert Bronze.crm_cust_info 
from 
  'C:\Users\tamer\Downloads\cust_info.csv' with (
    firstrow = 2, fieldterminator = ',', 
    ROWTERMINATOR = '\n', tablock
  );
SET 
  @END_TIME = GETDATE() PRINT 'LOADING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE Bronze.crm_prd_info ';
SET 
  @START_TIME = GETDATE() truncate table Bronze.crm_prd_info;
PRINT '>>INSERTING DATA INTO Bronze.crm_prd_info';
bulk insert Bronze.crm_prd_info 
from 
  "C:\Users\tamer\Downloads\dataset for youtube project\prd_info.csv" with (
    firstrow = 2, fieldterminator = ',', 
    tablock, rowterminator = '\n'
  ) 
SET 
  @END_TIME = GETDATE() PRINT 'LOADING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE Bronze.crm_sales_datails' 
SET 
  @START_TIME = GETDATE() truncate table Bronze.crm_sales_datails;
PRINT '>>INSERTING DATA INTO Bronze.crm_sales_datails';
bulk insert Bronze.crm_sales_datails 
from 
  "C:\Users\tamer\Downloads\dataset for youtube project\sales_details.csv" with (
    firstrow = 2, fieldterminator = ',', 
    rowterminator = '\n', tablock
  ) 
SET 
  @END_TIME = GETDATE() PRINT 'LOADING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
PRINT 'LOADING ERP TABLES';
PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE bronze.erp_cust_az';
SET 
  @START_TIME = GETDATE() truncate table bronze.erp_cust_az PRINT '>>INSERTING DATA INTO bronze.erp_cust_az ';
bulk insert bronze.erp_cust_az 
from 
  "C:\Users\tamer\Downloads\source_erp\CUST_AZ12.csv" with (
    firstrow = 2, fieldterminator = ',', 
    rowterminator = '\n', tablock
  ) 
SET 
  @END_TIME = GETDATE() PRINT 'LOADING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE bronze.erp_loc_A101';
SET 
  @START_TIME = GETDATE() truncate table bronze.erp_loc_A101 PRINT '>>INSERTING DATA INTO bronze.erp_loc_A101';
bulk insert bronze.erp_loc_A101 
from 
  "C:\Users\tamer\Downloads\source_erp\LOC_A101.csv" with (
    firstrow = 2, fieldterminator = ',', 
    rowterminator = '\n', tablock
  ) 
SET 
  @END_TIME = GETDATE() PRINT 'LODAING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
PRINT '>>>>TRUNCATING TABLE bronze.erp_px_cat_g1v2';
SET 
  @START_TIME = GETDATE() truncate table bronze.erp_px_cat_g1v2 PRINT '>>INSERTING DATA INTO bronze.erp_px_cat_g1v2 ';
bulk insert bronze.erp_px_cat_g1v2 
from 
  "C:\Users\tamer\Downloads\source_erp\PX_CAT_G1V2.csv" with (
    firstrow = 2, fieldterminator = ',', 
    rowterminator = '\n', tablock
  ) 
SET 
  @END_TIME = GETDATE() PRINT 'LODAING DURATION:' + CAST(
    DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR
  ) + ' SECONDS';
PRINT '----------------------------------';
SET 
  @BATCH_END_TIME = GETDATE();
PRINT 'BRONZE LAYER LOADING TIME: ' + CAST(
  DATEDIFF(
    SECOND, @BATCH_START_TIME, @BATCH_END_TIME
  ) AS NVARCHAR
) + ' SECONDS' END TRY BEGIN CATCH PRINT ' ERR0R OCCOURDED DURIN LOADING' PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
PRINT 'ERROR MESSAGE' + CAST(
  ERROR_NUMBER() AS NVARCHAR
);
PRINT 'ERROR MESSAGE ' + CAST (
  ERROR_STATE() AS NVARCHAR
);
PRINT 'ERROR MESSAGE' + CAST(
  ERROR_LINE() AS NVARCHAR
);
END CATCH END
