-- Source: wf_FF_AT_ZJ_GL_ACCOUNT.XML
{{ config(
    materialized='view',
    tags=['staging', 'sap_bw', 'gl_account']
) }}

-- Staging model for SAP BW OHS GL Account Input data
-- Replicates Informatica EXPTRANS transformation logic
-- Source: ZJGLAPTIN (SAP BW OHS DataSource)
-- Target: Standardized staging format with data cleansing

with source_data as (
    select * from {{ source('sap_bw_ohs', 'zjglaptin') }}
),

transformed as (
    select
        -- Pass-through fields (no transformation)
        ohrequid,
        datapakid,
        record,
        glani_zj as account,
        glmcu_zj as cost_center,
        gldoc_zj as journal_id,
        gldct_zj as document_type,
        gljeln_zj as journal_line_id,
        vendor_id_zj as vendor_id,
        vendor_name_zj as vendor_name,
        gmdl01_zj as account_description,
        glbcrc_zj as currency_code_base,
        glcrcd_zj as currency_code_from,
        glsbl_zj as subledger_gl,
        glpo_zj as purchase_order,
        glvinv_zj as supplier_invoice,
        fiscal_per_zj as fiscal_year_period,
        desc_long_zj as source_erp,
        
        -- Data cleansing transformations
        -- Replace double quotes in line description (REPLACESTR(1,in_LINE_DESC_ZJ,CHR(34),NULL))
        replace(line_desc_zj, '"', '') as journal_line_description,
        
        -- Replace double quotes in explanation and truncate to 30 chars (precision change from 60 to 30)
        left(replace(glexr_zj, '"', ''), 30) as remark_explanation,
        
        -- Date transformation: Convert SAP DATS to YYYYMMDD string format
        -- TO_CHAR(in_GLDGJ_ZJ, 'YYYYMMDD')
        case 
            when gldgj_zj is not null 
            then to_char(gldgj_zj, 'YYYYMMDD')
            else null
        end as posting_date,
        
        -- Amount transformations: TRUNC to 3 decimal places, precision change to 10,0
        -- trunc(in_AMOUNT_ZJ,3)
        case 
            when amount_zj is not null 
            then trunc(amount_zj, 3)::decimal(10,0)
            else null
        end as amount,
        
        -- trunc(in_AMOUNT_LOC_ZJ,3)
        case 
            when amount_loc_zj is not null 
            then trunc(amount_loc_zj, 3)::decimal(10,0)
            else null
        end as local_amount

    from source_data
)

select * from transformed

-- Data quality notes:
-- 1. Double quotes are removed from text fields to prevent CSV formatting issues
-- 2. Date fields are converted from SAP DATS format to string YYYYMMDD
-- 3. Decimal amounts are truncated to 3 decimal places as per source logic
-- 4. Field names are standardized to business-friendly names
-- 5. All transformations preserve original Informatica EXPTRANS logic