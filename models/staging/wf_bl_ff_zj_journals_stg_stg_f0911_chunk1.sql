-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
-- Staging model for Account Ledger (F0911)
-- Cleanses and standardizes journal entry data

{{ config(
    materialized='view',
    tags=['staging', 'journal_entries']
) }}

select
    -- Primary key components
    trim(glkco) as glkco,
    trim(gldct) as gldct,
    gldoc,
    gldgj,
    gljeln,
    trim(glextl) as glextl,
    trim(gllt) as gllt,
    
    -- Item information
    glicu,
    trim(glicut) as glicut,
    
    -- Dates
    gldicj,
    gldsyj,
    glhdgj,
    
    -- Company and account
    trim(glco) as glco,
    trim(glani) as glani,
    trim(glam) as glam,
    trim(glaid) as glaid,
    trim(glmcu) as glmcu,
    trim(globj) as globj,
    trim(glsub) as glsub,
    trim(glsbl) as glsbl,
    trim(glsblt) as glsblt,
    
    -- Amounts and currency
    glaa,
    glu as glu,
    trim(glum) as glum,
    trim(glcrcd) as glcrcd,
    glcrr,
    glhcrr,
    trim(glcrrm) as glcrrm,
    
    -- Descriptions
    trim(glexa) as glexa,
    trim(glexr) as glexr,
    
    -- Status flags
    trim(glpost) as glpost,
    trim(glre) as glre,
    trim(glrcnd) as glrcnd,
    trim(glbre) as glbre,
    trim(glsumm) as glsumm,
    trim(glprge) as glprge,
    
    -- Reference information
    trim(glpo) as glpo,
    trim(glasid) as glasid,
    trim(gluser) as gluser,
    glupmj,
    glupmt,
    
    -- User and system fields
    trim(gltorg) as gltorg,
    
    -- Load timestamp
    current_timestamp as etl_load_dte

from {{ source('crpdta', 'f0911') }}

-- Data quality filters
where glkco is not null
  and gldct is not null
  and gldoc is not null
  and gldgj is not null
  and gljeln is not null
  and glextl is not null
  and gllt is not null