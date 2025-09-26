-- Source: wf_DI_ITEM_MTRL_MASTER.XML
-- Staging model for material attributes and text data
-- Handles data type conversions and basic transformations

{{ config(
    materialized='view',
    tags=['staging', 'material_master']
) }}

with source_data as (
    select
        -- Core identifiers
        Product as product_id,
        ProductDescription as product_description,
        ProductOldID as product_old_id,
        ProductType as product_type,
        ProductGroup as product_group,
        ProductHierarchy as product_hierarchy,
        
        -- Physical attributes
        cast(MaterialVolume as decimal(15,3)) as material_volume,
        cast(GrossWeight as decimal(15,3)) as gross_weight,
        cast(NetWeight as decimal(15,3)) as net_weight,
        WeightUnit as weight_unit,
        VolumeUnit as volume_unit,
        BaseUnit as base_unit,
        
        -- Dimensions and measurements
        SizeOrDimensionText as size_or_dimension_text,
        cast(UnitSpecificProductLength as decimal(15,3)) as unit_specific_product_length,
        cast(UnitSpecificProductWidth as decimal(15,3)) as unit_specific_product_width,
        cast(UnitSpecificProductHeight as decimal(15,3)) as unit_specific_product_height,
        ProductMeasurementUnit as product_measurement_unit,
        
        -- Classification and categorization
        Division as division,
        IndustrySector as industry_sector,
        ItemCategoryGroup as item_category_group,
        ExternalProductGroup as external_product_group,
        ProductStandardID as product_standard_id,
        BasicMaterial as basic_material,
        commodity as commodity,
        
        -- Manufacturing and supplier info
        ManufacturerNumber as manufacturer_number,
        ManufacturerPartProfile as manufacturer_part_profile,
        ProductManufacturerNumber as product_manufacturer_number,
        
        -- Purchasing attributes
        PurchaseOrderQuantityUnit as purchase_order_quantity_unit,
        PurchngValKey as purchasing_value_key,
        VarblPurOrdUnitIsActive as variable_pur_ord_unit_is_active,
        
        -- Quality and batch management
        Materqualdisckind as material_quality_disc_kind,
        case 
            when upper(IsBatchManagementRequired) = 'X' then true
            else false
        end as is_batch_management_required,
        case 
            when upper(ApprovedBatchRecReq) = 'X' then true
            else false
        end as approved_batch_rec_req,
        SerialNoExplicitnessLevel as serial_no_explicitness_level,
        
        -- Storage and shelf life
        cast(Ttlshelflife as integer) as total_shelf_life,
        cast(MinRemShelf as integer) as min_remaining_shelf,
        Strcond as storage_condition,
        Strgperc as storage_percentage,
        case 
            when upper(Temppcondindi) = 'X' then true
            else false
        end as temp_condition_indicator,
        RoundrlcalSLED as rounding_rule_calc_sled,
        
        -- Packaging attributes
        PckgMatertyp as packaging_material_type,
        MaterGrpPckgMater as material_group_packaging_material,
        HandlingUnitType as handling_unit_type,
        PackCode as pack_code,
        
        -- Hazardous material indicators
        Hazardmatnum as hazardous_material_number,
        contnrreq as container_requirement,
        
        -- Regulatory and compliance
        CountryOfOriginMaterial as country_of_origin_material,
        AuthorizationGroup as authorization_group,
        InternationalArticleNumberCat as international_article_number_cat,
        
        -- Document management
        Docnumwithodocmanagsys as doc_num_without_doc_manage_sys,
        DoctypwithoDocManasys as doc_type_without_doc_manage_sys,
        DocverwithoDocManasys as doc_version_without_doc_manage_sys,
        Docchngenumwithodocumanasys as doc_change_num_without_doc_manage_sys,
        PgenumdocwithoDocManasys as page_num_doc_without_doc_manage_sys,
        
        -- Labels and forms
        LabelType as label_type,
        LabelForm as label_form,
        Labdsgnoffc as lab_design_office,
        PgeFormatProducMemo as page_format_product_memo,
        
        -- Freight and logistics
        MaterialFrieghtGroup as material_freight_group,
        cast(QuantGrgiprnt as decimal(15,3)) as quantity_grgi_print,
        
        -- Product indicators (boolean flags)
        case when upper(IndiInBulkLiquid) = 'X' then true else false end as indi_in_bulk_liquid,
        case when upper(IndiHighlyViscous) = 'X' then true else false end as indi_highly_viscous,
        case when upper(EnvrmtRelevant) = 'X' then true else false end as environment_relevant,
        case when upper(CADIndicator) = 'X' then true else false end as cad_indicator,
        case when upper(PrdIndiExpDt) = 'X' then true else false end as product_indi_exp_date,
        
        -- Status and maintenance
        MaintSt as maintenance_status,
        CrossPlantStatus as cross_plant_status,
        CrossPlantStatusValidityDate as cross_plant_status_validity_date,
        case 
            when upper(IsMarkedForDeletion) = 'X' then true
            else false
        end as is_marked_for_deletion,
        
        -- Audit fields
        CreatedByUser as created_by_user,
        cast(CreationDate as date) as creation_date,
        cast(TimeOfCreation as time) as time_of_creation,
        -- Combine creation date and time
        cast(concat(CreationDate, ' ', coalesce(TimeOfCreation, '00:00:00')) as timestamp) as creation_datetime,
        LastChangedByUser as last_changed_by_user,
        cast(LastChangeDate as date) as last_change_date,
        cast(LastChangeDateTime as timestamp) as last_change_datetime,
        
        -- Load metadata
        current_timestamp() as dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from {{ source('material_master_xml', 'material_attr_txt') }}
),

-- Apply data quality transformations
transformed_data as (
    select
        *,
        -- Create combined creation datetime if not available
        coalesce(
            last_change_datetime,
            creation_datetime,
            current_timestamp()
        ) as effective_change_datetime,
        
        -- Standardize boolean indicators
        case 
            when product_id is null or trim(product_id) = '' then false
            else true
        end as has_valid_product_id,
        
        -- Calculate derived fields
        case 
            when gross_weight > 0 and net_weight > 0 then gross_weight - net_weight
            else null
        end as tare_weight,
        
        -- Material classification flags
        case 
            when material_freight_group is not null then true
            else false
        end as has_freight_classification
        
    from source_data
)

select * from transformed_data

-- Filter out records without valid product ID
where has_valid_product_id = true