-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Material Master Lookup Enrichment
-- Performs multiple lookups to enrich material data with various indicators and attributes

{{ config(
    materialized='view',
    tags=['material_master', 'lookups']
) }}

with base_data as (
    select * from {{ ref('wf_di_item_mtrl_master_int_material_joined_desc') }}
),

-- PVC Free Indicator Lookup
pvc_free_lookup as (
    select 
        key_of_obj,
        char_value as pvc_free_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'PVC_FREE_IND'
),

-- Rework Indicator Lookup  
rework_lookup as (
    select 
        key_of_obj,
        char_value as rework_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'REWORK_IND'
),

-- HYP Code Lookup
hyp_cd_lookup as (
    select 
        key_of_obj,
        char_value as hyp_cd
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'HYP_CD'
),

-- HYP Brand Description Lookup
hyp_brand_lookup as (
    select 
        key_of_obj,
        char_value as hyp_brand_desc
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'HYP_BRAND_DESC'
),

-- Dangerous Goods Indicator Lookup
danger_goods_lookup as (
    select 
        key_of_obj,
        char_value as danger_goods_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'DANGER_GOODS_IND'
),

-- Material Thickness Indicator Lookup
mtrl_thk_ind_lookup as (
    select 
        key_of_obj,
        char_value as mtrl_thk_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'MTRL_THK_IND'
),

-- Material Thickness Upper Limit Lookup
mtrl_thk_upr_lookup as (
    select 
        key_of_obj,
        char_value as mtrl_thk_upr_limit_str
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'MTRL_THK_UPR_LIMIT'
),

-- Material Thickness Lower Limit Lookup
mtrl_thk_lwr_lookup as (
    select 
        key_of_obj,
        char_value as mtrl_thk_lwr_limit_str
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'MTRL_THK_LWR_LIMIT'
),

-- Material Thickness Measurement Lookup
mtrl_thk_msrmt_lookup as (
    select 
        key_of_obj,
        char_value as mtrl_thk_msrmt
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'MTRL_THK_MSRMT'
),

-- Shipping Restriction Lookup
shpng_rstrctn_lookup as (
    select 
        key_of_obj,
        char_value as shpng_rstrctn
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'SHPNG_RSTRCTN'
),

-- Shipping Temperature Control Indicator Lookup
shpng_tmptr_lookup as (
    select 
        key_of_obj,
        char_value as shpng_tmptr_ctrl_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'SHPNG_TMPTR_CTRL_IND'
),

-- Scrap Item Indicator Lookup
scrap_itm_lookup as (
    select 
        key_of_obj,
        char_value as scrap_itm_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'SCRAP_ITM_IND'
),

-- Rebox WW Indicator Lookup
rebox_ww_lookup as (
    select 
        key_of_obj,
        char_value as rebox_ww_ind
    from {{ source('sap_raw', 'material_characteristics') }}
    where characteristic_name = 'REBOX_WW_IND'
),

-- Join all lookups to base data
enriched_data as (
    select 
        bd.*,
        
        -- Lookup results with null handling
        coalesce(pvc.pvc_free_ind, '') as pvc_free_ind,
        coalesce(rw.rework_ind, '') as rework_ind,
        coalesce(hyp.hyp_cd, '') as hyp_cd,
        coalesce(hb.hyp_brand_desc, '') as hyp_brand_desc,
        coalesce(dg.danger_goods_ind, '') as danger_goods_ind,
        coalesce(mti.mtrl_thk_ind, '') as mtrl_thk_ind,
        
        -- Convert string values to decimal for thickness limits
        case 
            when mtu.mtrl_thk_upr_limit_str is not null 
                and mtu.mtrl_thk_upr_limit_str != ''
            then cast(mtu.mtrl_thk_upr_limit_str as decimal(6,2))
            else null
        end as mtrl_thk_upr_limit,
        
        case 
            when mtl.mtrl_thk_lwr_limit_str is not null 
                and mtl.mtrl_thk_lwr_limit_str != ''
            then cast(mtl.mtrl_thk_lwr_limit_str as decimal(6,2))
            else null
        end as mtrl_thk_lwr_limit,
        
        coalesce(mtm.mtrl_thk_msrmt, '') as mtrl_thk_msrmt,
        coalesce(sr.shpng_rstrctn, '') as shpng_rstrctn,
        coalesce(stc.shpng_tmptr_ctrl_ind, '') as shpng_tmptr_ctrl_ind,
        coalesce(si.scrap_itm_ind, '') as scrap_itm_ind,
        coalesce(rww.rebox_ww_ind, '') as rebox_ww_ind
        
    from base_data bd
    
    -- Left joins to preserve all base records, using first value on multiple matches
    left join pvc_free_lookup pvc 
        on bd.product = pvc.key_of_obj
    left join rework_lookup rw 
        on bd.product = rw.key_of_obj
    left join hyp_cd_lookup hyp 
        on bd.product = hyp.key_of_obj
    left join hyp_brand_lookup hb 
        on bd.product = hb.key_of_obj
    left join danger_goods_lookup dg 
        on bd.product = dg.key_of_obj
    left join mtrl_thk_ind_lookup mti 
        on bd.product = mti.key_of_obj
    left join mtrl_thk_upr_lookup mtu 
        on bd.product = mtu.key_of_obj
    left join mtrl_thk_lwr_lookup mtl 
        on bd.product = mtl.key_of_obj
    left join mtrl_thk_msrmt_lookup mtm 
        on bd.product = mtm.key_of_obj
    left join shpng_rstrctn_lookup sr 
        on bd.product = sr.key_of_obj
    left join shpng_tmptr_lookup stc 
        on bd.product = stc.key_of_obj
    left join scrap_itm_lookup si 
        on bd.product = si.key_of_obj
    left join rebox_ww_lookup rww 
        on bd.product = rww.key_of_obj
)

select * from enriched_data