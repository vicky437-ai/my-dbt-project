-- Source: wf_AM_DI_CUSTOMER.XML
-- Staging model for partner function data
-- Maps to PTNR_FCTN source in the original workflow

{{ config(
    materialized='view'
) }}

with source_data as (
    select
        cust_nbr,
        sales_org_id,
        dstrbtn_chnl,
        division,
        ptnr_fctn_id,
        ptnr_cntr,
        dflt_ptnr,
        ptnr_desc,
        ptnr_cust_nbr,
        vend_acct_nbr,
        cntct_prsn_nbr,
        prsnl_nbr,
        etl_load_dt
    from {{ source('stg_mdg_cust', 'ptnr_fctn') }}
),

transformed as (
    select
        cust_nbr,
        sales_org_id,
        dstrbtn_chnl,
        division,
        ptnr_fctn_id,
        ptnr_cntr,
        dflt_ptnr,
        ptnr_desc,
        ptnr_cust_nbr,
        vend_acct_nbr,
        cntct_prsn_nbr,
        prsnl_nbr,
        etl_load_dt,
        -- Audit fields
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_batch_id
    from source_data
)

select * from transformed