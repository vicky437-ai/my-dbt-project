-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Base Material Data with Initial Enrichment
-- This model represents the upstream processing before the joiner

{{ config(
    materialized='view',
    tags=['material_master', 'base']
) }}

with xml_source as (
    select * from {{ ref('wf_di_item_mtrl_master_stg_xml_material_attr_txt') }}
),

-- Apply language filter (equivalent to flt_LANG_KEY)
filtered_data as (
    select *
    from xml_source
    where language = 'EN'  -- Filter for English language key
),

-- Pass through expression for language key processing
processed_data as (
    select 
        *,
        -- Add any language-specific processing here
        'EN' as processed_language_key
    from filtered_data
)

select * from processed_data