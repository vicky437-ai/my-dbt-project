-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Intermediate model for filtered material data
-- Equivalent to flt_LANG_KEY transformation

{{ config(
    materialized='view',
    tags=['intermediate', 'material_master', 'filtered']
) }}

with filtered_materials as (
    select *
    from {{ ref('wf_di_item_mtrl_master_stg_material_attr_txt') }}
    
    -- Filter for specific language keys (equivalent to flt_LANG_KEY)
    -- Assuming English (EN) as primary language, adjust as needed
    where language_key in ('EN', 'E', '1')
       or language_key is null  -- Include records without language specification
)

select * from filtered_materials