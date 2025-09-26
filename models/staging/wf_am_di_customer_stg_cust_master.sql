-- Source: wf_AM_DI_CUSTOMER.XML
-- Staging model for customer master data
-- Replicates the source qualifier and expression transformation logic from Informatica

{{ config(
    materialized='view',
    pre_hook=var('param_src_pre_sql', ''),
    post_hook=var('param_src_post_sql', '')
) }}

with source_data as (
    select
        cust_nbr,
        cntry_key,
        city,
        po_cd,
        region_key,
        street,
        tel_nbr_1,
        fax_nbr,
        industry_tp,
        uniform_rsrc_lctr,
        fscl_yr_vrnt,
        annual_sales,
        central_ordr_blk,
        attribute_9,
        created_by,
        created_dt,
        last_chg_dt,
        etl_load_dt
    from {{ source('stg_mdg_cust', 'cust_master') }}
    {% if var('param_sq_filter', '') != '' %}
    where {{ var('param_sq_filter') }}
    {% endif %}
),

transformed as (
    select
        cust_nbr,
        cntry_key,
        city,
        po_cd,
        region_key,
        street,
        tel_nbr_1,
        fax_nbr,
        industry_tp,
        uniform_rsrc_lctr,
        fscl_yr_vrnt,
        annual_sales,
        central_ordr_blk,
        -- Business logic from exp_EXPTRANS transformation
        case 
            when central_ordr_blk = '01' or central_ordr_blk = 'ZB' then 'I'
            when central_ordr_blk is null then 'A'
            else central_ordr_blk
        end as central_ordr_blk_transformed,
        attribute_9,
        created_by,
        created_dt,
        last_chg_dt,
        etl_load_dt,
        -- Audit fields
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_batch_id
    from source_data
)

select * from transformed