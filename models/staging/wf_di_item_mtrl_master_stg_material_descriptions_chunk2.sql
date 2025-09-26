-- Source: wf_DI_ITEM_MTRL_MASTER.XML
{{ config(
    materialized='view',
    tags=['staging', 'material_descriptions']
) }}

with descriptions as (
    select
        product0 as product,
        language,
        product_description
    from {{ source('xml_source', 'xml_md_material_attr_txt') }}
    where language = 'EN'  -- Filter for English descriptions only
      and product0 is not null
      and product_description is not null
)

select 
    product,
    language,
    product_description
from descriptions