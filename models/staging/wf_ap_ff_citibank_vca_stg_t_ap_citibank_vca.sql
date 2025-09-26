-- Source: wf_AP_FF_CITIBANK_VCA.XML
{{ config(
    materialized='view',
    tags=['staging', 'citibank', 'vca']
) }}

-- Staging model for T_AP_CITIBANK_VCA
-- Replicates the Source Qualifier transformation from Informatica
-- Applies basic data type conversions and null handling

with source_data as (
    select
        -- Basic string fields - direct mapping from source
        action_type,
        record_id,
        issuer_id,
        ica_nbr,
        bank_nbr,
        user_name,
        request_id,
        min_purchase_amt,
        max_purchase_amt,
        purchase_currency,
        purchase_type,
        v_card_alias,
        supplier_name,
        supplier_email,
        multi_use,
        
        -- Date fields - preserve original format for transformation layer
        valid_from,
        valid_to,
        valid_for,
        
        -- CDF fields
        cdf_payment_number,
        cdf_payee_number,
        cdf_payment_amount,
        cdf_payment_date,
        cdf_payee_name,
        
        -- Audit field
        etl_load_dte
        
    from {{ source('t_ap', 't_ap_citibank_vca') }}
    
    -- Apply source filter if parameter is provided (equivalent to Informatica Source Filter)
    {% if var('source_filter', '') != '' %}
    where {{ var('source_filter') }}
    {% endif %}
)

select * from source_data

-- Add source-level data quality checks
-- Equivalent to Informatica error handling
where record_id is not null
  and action_type is not null
  and request_id is not null
  and valid_from is not null
  and valid_to is not null