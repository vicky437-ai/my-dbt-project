-- Source: wf_FF_AT_ZJ_GL_ACCOUNT.XML
{{ config(
    materialized='view',
    tags=['intermediate', 'gl_account', 'data_quality']
) }}

-- Intermediate model for additional data quality checks and business rules
-- Applied after staging transformations

with staged_data as (
    select * from {{ ref('wf_ff_at_zj_gl_account_stg_sap_bw_ohs__zjglaptin') }}
),

data_quality_enhanced as (
    select
        *,
        
        -- Add data quality flags
        case 
            when account is null or trim(account) = '' 
            then 'MISSING_ACCOUNT'
            else 'VALID'
        end as account_quality_flag,
        
        case 
            when posting_date is null or length(posting_date) != 8
            then 'INVALID_DATE'
            else 'VALID'
        end as date_quality_flag,
        
        case 
            when amount is null and local_amount is null
            then 'MISSING_AMOUNTS'
            when amount = 0 and local_amount = 0
            then 'ZERO_AMOUNTS'
            else 'VALID'
        end as amount_quality_flag,
        
        -- Business logic enhancements
        case 
            when currency_code_base = currency_code_from 
            then 'NO_CONVERSION'
            when currency_code_base is null or currency_code_from is null
            then 'MISSING_CURRENCY'
            else 'CURRENCY_CONVERSION'
        end as currency_conversion_type,
        
        -- Extract fiscal year and period
        case 
            when length(fiscal_year_period) >= 4
            then left(fiscal_year_period, 4)
            else null
        end as fiscal_year,
        
        case 
            when length(fiscal_year_period) >= 6
            then right(fiscal_year_period, 2)
            else null
        end as fiscal_period

    from staged_data
)

select * from data_quality_enhanced

-- This intermediate layer adds:
-- 1. Data quality flags for monitoring
-- 2. Business logic for currency conversion detection
-- 3. Fiscal year/period parsing
-- 4. Preparation for final mart layer