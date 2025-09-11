use DataWarehouse;
go


create view gold.dim_customers
as 
select 
    row_number() over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as customer_firstname,
    ci.cst_lastname as customer_lastname,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case 
        when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else coalesce(ca.gen, 'n/a')
    end as gender,
    ca.bdate as birthdate
from silver.crm_cst_info as ci
left join silver.erp_cst_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;

go




create view gold.dim_products
as
select 
    row_number() over(order by pn.prd_key, pn.prd_start_dt) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,

    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance,

    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
  
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null

go



create view gold.fact_sales
as
SELECT 
    sd.sls_ord_num as order_number,

    pr.product_key,
    cu.customer_key,

    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quatity,
    sd.sls_price as price
FROM silver.crm_sales_details as sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cst_id = cu.customer_id
--order by customer_key
go


on p.product_key = f.product_key
where p.product_key is null
