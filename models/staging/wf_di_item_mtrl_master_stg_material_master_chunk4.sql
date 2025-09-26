-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Staging model for material master data
-- Implements exp_PASS_THROUGH transformation logic

{{ config(
    materialized='view',
    tags=['staging', 'material_master']
) }}

with source_data as (
    select * from {{ source('sap_source', 'material_master') }}
),

transformed as (
    select
        -- Product with trimming (LTRIM/RTRIM equivalent)
        trim(Product) as product,
        
        -- Pass-through fields
        MaterGrpPckgMater as material_group_packaging,
        ProductGroup as product_group,
        WeightUnit as weight_unit,
        VolumeUnit as volume_unit,
        MaterialVolume as material_volume,
        SizeOrDimensionText as size_or_dimension_text,
        ProductHierarchy as product_hierarchy,
        ProductOldID as product_old_id,
        NetWeight as net_weight,
        Labdsgnoffc as lab_design_office,
        ProductStandardID as product_standard_id,
        GrossWeight as gross_weight,
        ItemCategoryGroup as item_category_group,
        ExternalProductGroup as external_product_group,
        Division as division,
        CrossPlantStatusValidityDate as cross_plant_status_validity_date,
        CrossPlantStatus as cross_plant_status,
        InternationalArticleNumberCat as international_article_number_cat,
        AuthorizationGroup as authorization_group,
        ProductType as product_type,
        IndustrySector as industry_sector,
        PgenumdocwithoDocManasys as page_num_doc_without_doc_mgmt_sys,
        IndustryStandardName as industry_standard_name,
        DocverwithoDocManasys as doc_version_without_doc_mgmt_sys,
        DoctypwithoDocManasys as doc_type_without_doc_mgmt_sys,
        Docchngenumwithodocumanasys as doc_change_num_without_doc_mgmt_sys,
        
        -- Boolean conversions using UDF equivalent
        {{ boolean_to_string('in_CADIndicator') }} as cad_indicator,
        
        BasicMaterial as basic_material,
        PgeFormatProducMemo as page_format_product_memo,
        
        -- More boolean conversions
        {{ boolean_to_string('in_IndiInBulkLiquid') }} as indi_in_bulk_liquid,
        {{ boolean_to_string('in_IndiHighlyViscous') }} as indi_highly_viscous,
        {{ boolean_to_string('in_EnvrmtRelevant') }} as environment_relevant,
        
        Docnumwithodocmanagsys as doc_num_without_doc_mgmt_sys,
        PackCode as pack_code,
        
        {{ boolean_to_string('in_ApprovedBatchRecReq') }} as approved_batch_rec_req,
        
        Ttlshelflife as total_shelf_life,
        Temppcondindi as temp_condition_indicator,
        Strgperc as storage_percentage,
        Strcond as storage_condition,
        RoundrlcalSLED as round_rule_calc_sled,
        Hazardmatnum as hazard_material_number,
        contnrreq as container_requirement,
        PrdIndiExpDt as product_indi_exp_date,
        QuantGrgiprnt as quantity_gross_imprint,
        MinRemShelf as min_remaining_shelf,
        LabelType as label_type,
        LabelForm as label_form,
        SerialNoExplicitnessLevel as serial_no_explicitness_level,
        ManufacturerNumber as manufacturer_number,
        VarblPurOrdUnitIsActive as variable_pur_ord_unit_is_active,
        PurchngValKey as purchasing_value_key,
        Materqualdisckind as material_quality_disc_kind,
        PurchaseOrderQuantityUnit as purchase_order_quantity_unit,
        ManufacturerPartProfile as manufacturer_part_profile,
        ProductManufacturerNumber as product_manufacturer_number,
        MaterialFrieghtGroup as material_freight_group,
        CreatedByUser as created_by_user,
        
        -- DateTime combination logic
        case 
            when CreationDate is not null then
                cast(
                    substr(cast(CreationDate as string), 1, 10) || ' ' || 
                    substr(cast(TimeOfCreation as string), 12, 8) 
                    as timestamp
                )
            else null
        end as creation_datetime,
        
        LastChangedByUser as last_changed_by_user,
        
        -- Last change datetime combination
        cast(
            substr(cast(LastChangeDate as string), 1, 10) || ' ' || 
            substr(cast(LastChangeDateTime as string), 12, 8) 
            as timestamp
        ) as last_change_datetime,
        
        BaseUnit as base_unit,
        
        -- Batch management conversion
        case 
            when IsBatchManagementRequired = 1 then 'Y'
            when IsBatchManagementRequired = 0 then 'N'
            else null
        end as is_batch_management_required,
        
        MaintSt as maintenance_status,
        commodity as commodity,
        CountryOfOriginMaterial as country_of_origin_material,
        ProductDescription as product_description,
        
        -- Width conversion with decimal casting
        cast(UnitSpecificProductWidth as decimal(13,3)) as unit_specific_product_width,
        UnitSpecificProductLength as unit_specific_product_length,
        UnitSpecificProductHeight as unit_specific_product_height,
        
        PckgMatertyp as packaging_material_type,
        HandlingUnitType as handling_unit_type,
        ProductMeasurementUnit as product_measurement_unit,
        
        -- Soft delete conversion
        {{ soft_delete_conversion('IsMarkedForDeletion') }} as is_marked_for_deletion,
        
        -- Generate MD5 for change detection
        {{ generate_md5_hash([
            'trim(Product)', 'MaterGrpPckgMater', 'ProductGroup', 'WeightUnit',
            'VolumeUnit', 'MaterialVolume', 'NetWeight', 'GrossWeight',
            'BaseUnit', 'ProductDescription', 'LastChangedByUser'
        ]) }} as source_md5_hash,
        
        current_timestamp() as dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from source_data
)

select * from transformed