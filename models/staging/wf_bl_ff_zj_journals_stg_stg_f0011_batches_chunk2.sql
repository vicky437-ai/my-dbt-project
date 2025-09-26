-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
{{ config(materialized='view') }}

select
    case 
        when trim(icicu) is null then null
        else cast(replace(trim(icicu), '"', '') as number)
    end as icicu,
    
    case 
        when trim(icicut) is null then ''
        else replace(trim(icicut), '"', '')
    end as icicut,
    
    case 
        when trim(icist) is null then ''
        else replace(trim(icist), '"', '')
    end as icist,
    
    case 
        when trim(icame) is null then null
        else cast(replace(trim(icame), '"', '') as number)
    end as icame
    
from {{ source('jde_source', 'f0011') }}