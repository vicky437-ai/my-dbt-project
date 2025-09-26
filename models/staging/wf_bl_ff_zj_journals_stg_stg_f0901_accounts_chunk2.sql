-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
{{ config(materialized='view') }}

select
    case 
        when trim(gmaid) is null then ''
        else replace(trim(gmaid), '"', '')
    end as gmaid,
    
    case 
        when trim(gmdl01) is null then ''
        else replace(trim(gmdl01), '"', '')
    end as gmdl01
    
from {{ source('jde_source', 'f0901') }}