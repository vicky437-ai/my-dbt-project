-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Material Master with Product Description Join
-- Joins material data with product descriptions using sorted input

{{ config(
    materialized='view',
    tags=['material_master', 'description']
) }}

with material_data as (
    select * from {{ ref('wf_di_item_mtrl_master_int_material_base_enriched') }}
),

-- Sort product descriptions for consistent joining
product_descriptions as (
    select 
        product,
        product_description
    from {{ source('sap_raw', 'product_text') }}
    where language = 'EN'  -- Filter for English language key
    order by product asc
),

-- Join material data with descriptions using normal join (inner join)
-- This preserves the Informatica joiner behavior with "Normal Join"
joined_data as (
    select 
        md.*,
        pd.product_description
    from material_data md
    inner join product_descriptions pd
        on md.product = pd.product
)

select * from joined_data