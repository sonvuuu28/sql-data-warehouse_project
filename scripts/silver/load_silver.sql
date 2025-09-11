use DataWarehouse;
go

create or alter procedure silver.load_silver as
	begin
	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.crm_cst_info'
	truncate table silver.crm_cst_info

	PRINT'>> Inserting table: silver.crm_cst_info'
	insert into silver.crm_cst_info (
		cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
	)

	select cst_id, cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, 
			case 
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end cst_marital_status,	

			case 
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
			end cst_gndr,	

			cst_create_date
	from 
	(
		select *, ROW_NUMBER() over (partition by cst_key order by cst_create_date desc) as flag_last_t2 
		from (
			select *, ROW_NUMBER() over (partition by cst_key order by cst_create_date desc) as flag_last 
			from bronze.crm_cst_info
			where cst_id is not null
		) as t
		where flag_last = 1
	) as t2
	where flag_last_t2 = 1


	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.crm_prd_info'
	truncate table silver.crm_prd_info

	PRINT'>> Inserting table: silver.crm_prd_info'
	insert into silver.crm_prd_info(
		prd_id,
		prd_key,
		cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
		prd_id,
		substring(prd_key, 7, len(prd_key)) as prd_key, 
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id, --dựa trên erp của category
		prd_nm,
		isNull(prd_cost, 0) as prd_cost,
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line, 
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt_test
	from bronze.crm_prd_info

	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.crm_sales_details'
	truncate table silver.crm_sales_details

	PRINT'>> Inserting table: silver.crm_sales_details'
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cst_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cst_id,
		case
			when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
			else cast(cast(sls_order_dt as nvarchar) as date)
		end as sls_order_dt,

		case
			when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
			else cast(cast(sls_ship_dt as nvarchar) as date)
		end as sls_ship_dt,

		case
			when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
			else cast(cast(sls_due_dt as nvarchar) as date)
		end as sls_due_dt,

		case 
			when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,

		sls_quantity,

		case 
			when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity, 0) 
			else sls_price
		end as sls_price
	from bronze.crm_sales_details

	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.erp_cst_az12'
	truncate table silver.erp_cst_az12

	PRINT'>> Inserting table: silver.erp_cst_az12'
	insert into silver.erp_cst_az12(
		cid,
		bdate,
		gen
	)
	select 
		case 
			when cid like 'AW%' then cid
			else substring(cid,4,len(cid))
		end as cid,

		case
			when bdate > getdate() then null
			else bdate
		end as bdate,

		case upper(trim(gen)) 
			when 'F' then 'Female'
			when 'M' then 'Male'
			when '' then 'n/a'
			else 'n/a'
		end as gen
	from bronze.erp_cst_az12


	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.erp_loc_a101'
	truncate table silver.erp_loc_a101

	PRINT'>> Inserting table: silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
		cid,cntry
	)
	select 
		replace(cid, '-', '') as cid,
		case 
			when upper(trim(cntry)) in ('USA', 'US') then 'United States'
			when upper(trim(cntry)) = 'DE' then 'Germany'
			when upper(trim(cntry)) = '' or cntry is null then 'n/a' 
			else cntry
		end as cntry
	from bronze.erp_loc_a101


	----------------------------------------------------------------------------------------------------
	PRINT'>> Truncating table: silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2

	PRINT'>> Inserting table: silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2
	(
		id, cat,subcat, maintenance
	)
	select * from bronze.erp_px_cat_g1v2;

end;