-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Staging model for Material Attributes lookup data
-- Used for various attribute lookups in the original workflow

{{ config(
    materialized='view',
    tags=['staging', 'material_master', 'lookup']
) }}

with source_data as (
    select
        material_number,
        attribute_name,
        attribute_value,
        
        -- ETL metadata
        current_timestamp as load_timestamp,
        '{{ invocation_id }}' as batch_id
        
    from {{ source('lookup_tables', 'material_attributes') }}
    
    where material_number is not null
      and attribute_name is not null
)

select * from source_data