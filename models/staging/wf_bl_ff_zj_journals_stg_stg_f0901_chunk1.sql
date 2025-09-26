-- Source: wf_BL_FF_ZJ_JOURNALS_STG.XML
-- Staging model for Account Master (F0901)
-- Cleanses and standardizes account master data

{{ config(
    materialized='view',
    tags=['staging', 'account_master']
) }}

select
    -- Primary key
    trim(gmaid) as gmaid,
    
    -- Company information
    trim(gmco) as gmco,
    trim(gmmcu) as gmmcu,
    
    -- Account structure
    trim(gmobj) as gmobj,
    trim(gmsub) as gmsub,
    trim(gmans) as gmans,
    
    -- Descriptions
    trim(gmdl01) as gmdl01,
    
    -- Flags and codes
    trim(gmlda) as gmlda,
    trim(gmbpc) as gmbpc,
    trim(gmpec) as gmpec,
    trim(gmbill) as gmbill,
    trim(gmcrcd) as gmcrcd,
    trim(gmum) as gmum,
    
    -- Reporting codes
    trim(gmr001) as gmr001,
    trim(gmr002) as gmr002,
    trim(gmr003) as gmr003,
    trim(gmr004) as gmr004,
    trim(gmr005) as gmr005,
    
    -- Audit fields
    trim(gmuser) as gmuser,
    trim(gmpid) as gmpid,
    trim(gmjobn) as gmjobn,
    gmupmj,
    gmupmt,
    
    -- Load timestamp
    current_timestamp as etl_load_dte

from {{ source('crpdta', 'f0901') }}

-- Data quality filters
where gmaid is not null
  and trim(gmaid) != ''