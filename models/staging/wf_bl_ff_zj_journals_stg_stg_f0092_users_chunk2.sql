-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
{{ config(materialized='view') }}

select
    case 
        when trim(uluser) is null then ''
        else replace(trim(uluser), '"', '')
    end as uluser,
    
    case 
        when trim(ulan8) is null then null
        else cast(replace(trim(ulan8), '"', '') as number)
    end as ulan8
    
from {{ source('jde_source', 'f0092') }}