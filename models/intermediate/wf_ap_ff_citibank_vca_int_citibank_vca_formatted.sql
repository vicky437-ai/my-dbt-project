-- Source: wf_AP_FF_CITIBANK_VCA.XML
{{ config(
    materialized='view',
    tags=['intermediate', 'citibank', 'vca', 'formatted']
) }}

-- Intermediate model that replicates the exp_SET_COLUMNS transformation
-- Formats date fields and generates dynamic filename with timestamp
-- Preserves exact Informatica transformation logic

with formatted_data as (
    select
        -- Pass-through fields (no transformation needed)
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
        valid_for,
        cdf_payment_number,
        cdf_payee_number,
        cdf_payment_amount,
        cdf_payment_date,
        cdf_payee_name,
        
        -- Original date fields for reference
        valid_from as valid_from_original,
        valid_to as valid_to_original,
        
        -- Formatted date fields - exact replication of Informatica expressions
        -- Expression: to_char(VALID_FROM,'YYYY-MM-DD HH24:MI:SS')||'.00'||' +0000'
        case 
            when valid_from is not null then
                to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS') || '.00 +0000'
            else null
        end as out_valid_from,
        
        -- Expression: to_char(VALID_TO,'YYYY-MM-DD HH24:MI:SS')||'.00'||' +0000'
        case 
            when valid_to is not null then
                to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS') || '.00 +0000'
            else null
        end as out_valid_to,
        
        -- Dynamic filename generation - exact replication of Informatica expression
        -- Expression: 'ZIMMERVCAI_FPCRULX_US_'||TO_CHAR(SYSDATE,'YYYYMMDD')||'_'||TO_CHAR(SYSDATE,'HHMISS')||'.csv'
        concat(
            '{{ var("file_prefix") }}',
            to_char(current_timestamp, 'YYYYMMDD'),
            '_',
            to_char(current_timestamp, 'HH24MISS'),
            '{{ var("file_extension") }}'
        ) as out_file_name,
        
        -- Add processing timestamp for audit
        current_timestamp as processed_at
        
    from {{ ref('wf_ap_ff_citibank_vca_stg_t_ap_citibank_vca') }}
)

select * from formatted_data