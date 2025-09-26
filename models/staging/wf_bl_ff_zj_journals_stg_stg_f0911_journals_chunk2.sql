-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
{{ config(
    materialized='view',
    pre_hook="{{ log('Processing F0911 journal entries', info=true) }}"
) }}

with source_data as (
    select
        -- Clean and handle null values, remove double quotes
        case 
            when trim(glicu) is null then null
            else cast(replace(trim(glicu), '"', '') as number)
        end as glicu,
        
        case 
            when trim(glicut) is null then ''
            else replace(trim(glicut), '"', '')
        end as glicut,
        
        case 
            when trim(gldct) is null then ''
            else replace(trim(gldct), '"', '')
        end as gldct,
        
        case 
            when trim(gldoc) is null then null
            else cast(replace(trim(gldoc), '"', '') as number)
        end as gldoc,
        
        case 
            when trim(glkco) is null then ''
            else replace(trim(glkco), '"', '')
        end as glkco,
        
        case 
            when trim(gldgj) is null then null
            else cast(replace(trim(gldgj), '"', '') as number)
        end as gldgj,
        
        case 
            when trim(gllt) is null then ''
            else replace(trim(gllt), '"', '')
        end as gllt,
        
        case 
            when trim(glexa) is null then ''
            else replace(trim(glexa), '"', '')
        end as glexa,
        
        case 
            when trim(glcrcd) is null then ''
            else replace(trim(glcrcd), '"', '')
        end as glcrcd,
        
        case 
            when trim(glcrrm) is null then ''
            else replace(trim(glcrrm), '"', '')
        end as glcrrm,
        
        case 
            when trim(gltorg) is null then ''
            else replace(trim(gltorg), '"', '')
        end as gltorg,
        
        case 
            when trim(gldicj) is null then null
            else cast(replace(trim(gldicj), '"', '') as number)
        end as gldicj,
        
        case 
            when trim(gljeln) is null then null
            else cast(replace(trim(gljeln), '"', '') as number)
        end as gljeln,
        
        case 
            when trim(glextl) is null then ''
            else replace(trim(glextl), '"', '')
        end as glextl,
        
        case 
            when trim(glani) is null then ''
            else replace(trim(glani), '"', '')
        end as glani,
        
        case 
            when trim(glco) is null then ''
            else replace(trim(glco), '"', '')
        end as glco,
        
        case 
            when trim(glaa) is null then null
            else cast(replace(trim(glaa), '"', '') as number)
        end as glaa,
        
        case 
            when trim(glexr) is null then ''
            else replace(trim(glexr), '"', '')
        end as glexr,
        
        case 
            when trim(glsblt) is null then ''
            else replace(trim(glsblt), '"', '')
        end as glsblt,
        
        case 
            when trim(glsbl) is null then ''
            else replace(trim(glsbl), '"', '')
        end as glsbl,
        
        case 
            when trim(glpost) is null then ''
            else replace(trim(glpost), '"', '')
        end as glpost,
        
        case 
            when trim(glre) is null then ''
            else replace(trim(glre), '"', '')
        end as glre,
        
        case 
            when trim(glrcnd) is null then ''
            else replace(trim(glrcnd), '"', '')
        end as glrcnd,
        
        case 
            when trim(glpo) is null then ''
            else replace(trim(glpo), '"', '')
        end as glpo,
        
        case 
            when trim(glasid) is null then ''
            else replace(trim(glasid), '"', '')
        end as glasid,
        
        case 
            when trim(gluser) is null or trim(gluser) = '' then ''
            else replace(trim(gluser), '"', '')
        end as gluser,
        
        case 
            when trim(glupmj) is null then null
            else cast(replace(trim(glupmj), '"', '') as number)
        end as glupmj,
        
        case 
            when trim(glupmt) is null then null
            else cast(replace(trim(glupmt), '"', '') as number)
        end as glupmt,
        
        current_timestamp as etl_load_dte
        
    from {{ source('jde_source', 'f0911') }}
)

select * from source_data