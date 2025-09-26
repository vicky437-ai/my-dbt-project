-- Source: wf_FF_AT_Z1_GL_ACCOUNT.XML
{{ config(
    materialized='view',
    tags=['intermediate', 'finance', 'gl_accounts']
) }}

/*
    Intermediate model to enhance GL account data with additional business logic
    
    This model adds:
    - Standardized date formatting
    - Amount sign handling based on debit/credit indicator
    - Data quality flags
    - Enhanced categorization
*/

with staging_data as (
    select * from {{ ref('wf_ff_at_z1_gl_account_stg_gl_account_transactions') }}
),

enhanced_data as (
    select
        -- Original cleaned fields
        posting_date_clean,
        fiscal_year_clean,
        fiscal_period_clean,
        accounting_document_number_clean,
        line_item_number_clean,
        item_text_clean,
        gl_account_number_clean,
        gl_account_description_clean,
        cost_center_clean,
        company_code_clean,
        vendor_account_clean,
        supplier_name_clean,
        purchase_order_clean,
        invoice_reference_clean,
        amount_document_currency_str,
        currency_key_clean,
        amount_local_currency_str,
        local_currency_clean,
        source_description_clean,
        project_definition_clean,
        wbs_clean,
        document_type_clean,
        debit_credit_indicator,
        gb_source,
        
        -- Enhanced fields for business logic
        case 
            when debit_credit_indicator = 'S' then 'Debit'
            when debit_credit_indicator = 'H' then 'Credit'
            else 'Unknown'
        end as debit_credit_description,
        
        -- Convert amounts back to numeric for calculations if needed
        cast(amount_document_currency_str as decimal(23,3)) as amount_document_currency_numeric,
        cast(amount_local_currency_str as decimal(23,3)) as amount_local_currency_numeric,
        
        -- Data quality flags
        case 
            when vendor_account_clean is not null and vendor_account_clean != '' then true
            else false
        end as has_vendor_info,
        
        case 
            when purchase_order_clean is not null and purchase_order_clean != '' then true
            else false
        end as has_purchase_order,
        
        case 
            when project_definition_clean is not null and project_definition_clean != '' then true
            else false
        end as has_project_info,
        
        -- Audit fields
        dbt_loaded_at,
        dbt_batch_id,
        current_timestamp() as dbt_enhanced_at
        
    from staging_data
)

select * from enhanced_data