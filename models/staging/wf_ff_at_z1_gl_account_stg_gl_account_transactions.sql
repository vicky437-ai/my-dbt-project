-- Source: wf_FF_AT_Z1_GL_ACCOUNT.XML
{{ config(
    materialized='view',
    tags=['staging', 'finance', 'gl_accounts']
) }}

/*
    Staging model for GL Account transactions from BW datasource
    
    This model replicates the Informatica exp_PASSTHROUGH transformation logic:
    - Trims whitespace from all string fields
    - Removes double quotes from text fields to prevent CSV issues
    - Converts decimal amounts to strings with proper formatting
    - Preserves all source data with proper cleansing
    
    Source: ZB_REPORTING.FINANCE.GLOBAL.APPTIO/GB_Z1_APPT1
    Target: Staging layer for GL account data
*/

with source_data as (
    select * from {{ source('zb_reporting_finance_global_apptio', 'gb_z1_appt1') }}
    {% if var('param_sq_filter', '') != '' %}
    where {{ var('param_sq_filter') }}
    {% endif %}
),

cleaned_data as (
    select
        -- Date and period fields with trimming (equivalent to LTRIM(RTRIM()))
        trim(postingdateinthedocument) as posting_date_clean,
        trim(fiscalyear) as fiscal_year_clean,
        trim(fiscalperiod) as fiscal_period_clean,
        
        -- Document identification fields
        trim(accountingdocumentnumber) as accounting_document_number_clean,
        trim(numoflineitemwithinaccountdoc) as line_item_number_clean,
        
        -- Text fields with quote removal (equivalent to REPLACECHR(0,LTRIM(RTRIM(field)),'"',NULL))
        replace(trim(itemtext), '"', '') as item_text_clean,
        
        -- Account and organizational fields
        trim(glaccountnumber) as gl_account_number_clean,
        replace(trim(glaccountlongname), '"', '') as gl_account_description_clean,
        trim(costcenter) as cost_center_clean,
        trim(companycode) as company_code_clean,
        
        -- Vendor fields
        trim(accountnumofvendororcreditor) as vendor_account_clean,
        replace(trim(supplierfullname), '"', '') as supplier_name_clean,
        
        -- Purchase and invoice fields
        trim(purchasingdocumentnumber) as purchase_order_clean,
        trim(invoice_reference) as invoice_reference_clean,
        
        -- Amount fields converted to string (equivalent to TO_CHAR(TO_DECIMAL(field,3)))
        cast(cast(amountindocumentcurrency as decimal(23,3)) as string) as amount_document_currency_str,
        trim(currencykey) as currency_key_clean,
        cast(cast(amountinlocalcurrency as decimal(23,3)) as string) as amount_local_currency_str,
        trim(localcurrency) as local_currency_clean,
        
        -- System and project fields
        trim(source_desc_long) as source_description_clean,
        trim(projectdefinition) as project_definition_clean,
        trim(wbs) as wbs_clean,
        trim(documenttype) as document_type_clean,
        
        -- Passthrough fields (no transformation needed)
        debitcreditindicator as debit_credit_indicator,
        
        -- Note: GBSOURCE field referenced in transformation but not in source - setting as null
        cast(null as string) as gb_source,
        
        -- Audit fields for data lineage
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_batch_id
        
    from source_data
)

select * from cleaned_data

-- Data quality checks inline
-- Ensure critical fields are not null after cleaning
where posting_date_clean is not null
  and fiscal_year_clean is not null
  and fiscal_period_clean is not null
  and accounting_document_number_clean is not null
  and line_item_number_clean is not null
  and gl_account_number_clean is not null
  and company_code_clean is not null