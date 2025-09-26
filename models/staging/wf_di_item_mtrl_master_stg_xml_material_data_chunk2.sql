-- Source: wf_DI_ITEM_MTRL_MASTER.XML
{{ config(
    materialized='view',
    tags=['staging', 'material_master']
) }}

with source_data as (
    select
        -- Core product information
        product,
        product_type,
        product_group,
        base_unit,
        weight_unit,
        volume_unit,
        cast(material_volume as decimal(13,3)) as material_volume,
        size_or_dimension_text,
        product_hierarchy,
        product_old_id,
        cast(net_weight as decimal(13,3)) as net_weight,
        labdsgnoffc,
        product_standard_id,
        cast(gross_weight as decimal(13,3)) as gross_weight,
        item_category_group,
        external_product_group,
        division,
        cross_plant_status_validity_date,
        cross_plant_status,
        international_article_number_cat,
        authorization_group,
        industry_sector,
        pgenumdocwitho_doc_manasys,
        industry_standard_name,
        docverwitho_doc_manasys,
        doctypwitho_doc_manasys,
        docchngenumwithodocumanasys,
        
        -- Boolean conversions using custom macro
        {{ boolean_to_string('cad_indicator') }} as cad_indicator,
        
        basic_material,
        pge_format_produc_memo,
        
        -- More boolean conversions
        {{ boolean_to_string('indi_in_bulk_liquid') }} as indi_in_bulk_liquid,
        {{ boolean_to_string('indi_highly_viscous') }} as indi_highly_viscous,
        {{ boolean_to_string('envrmnt_relevant') }} as envrmnt_relevant,
        
        docnumwithodocmanagsys,
        pack_code,
        
        {{ boolean_to_string('approved_batch_rec_req') }} as approved_batch_rec_req,
        
        cast(ttlshelflife as decimal(4,0)) as ttlshelflife,
        temppcondindi,
        cast(strgperc as decimal(3,0)) as strgperc,
        strcond,
        roundrlcal_sled,
        hazardmatnum,
        contnrreq,
        prd_indi_exp_dt,
        cast(quant_grgiprnt as decimal(13,3)) as quant_grgiprnt,
        cast(min_rem_shelf as decimal(4,0)) as min_rem_shelf,
        label_type,
        label_form,
        serial_no_explicitness_level,
        manufacturer_number,
        varbl_pur_ord_unit_is_active,
        purchng_val_key,
        materqualdisckind,
        purchase_order_quantity_unit,
        manufacturer_part_profile,
        product_manufacturer_number,
        material_frieght_group,
        created_by_user,
        last_changed_by_user,
        maint_st,
        commodity,
        country_of_origin_material,
        pckg_matertyp,
        handling_unit_type,
        cast(unit_specific_product_length as decimal(13,3)) as unit_specific_product_length,
        cast(unit_specific_product_width as decimal(13,3)) as unit_specific_product_width,
        cast(unit_specific_product_height as decimal(13,3)) as unit_specific_product_height,
        product_measurement_unit,
        
        -- Date/time combinations
        case 
            when creation_date is not null then
                cast(
                    concat(
                        substr(cast(creation_date as string), 1, 10),
                        ' ',
                        substr(cast(time_of_creation as string), 12, 8)
                    ) as timestamp
                )
            else null
        end as creation_date_time,
        
        cast(
            concat(
                substr(cast(last_change_date as string), 1, 10),
                ' ',
                substr(cast(last_change_date_time as string), 12, 8)
            ) as timestamp
        ) as last_change_date_time,
        
        -- Batch management flag conversion
        case 
            when is_batch_management_required = 1 then 'Y'
            when is_batch_management_required = 0 then 'N'
            else null
        end as is_batch_management_required,
        
        -- Soft delete flag conversion
        {{ soft_delete_flag('is_marked_for_deletion') }} as is_marked_for_deletion
        
    from {{ source('xml_source', 'xml_md_material_attr_txt') }}
    where product is not null
),

cleaned_data as (
    select
        *,
        -- Clean product number
        trim(product) as product_clean,
        
        -- Generate source MD5 for change detection
        {{ generate_md5_hash([
            'product', 'product_type', 'base_unit', 'gross_weight', 
            'net_weight', 'creation_date_time', 'last_change_date_time'
        ]) }} as src_md5
        
    from source_data
)

select * from cleaned_data