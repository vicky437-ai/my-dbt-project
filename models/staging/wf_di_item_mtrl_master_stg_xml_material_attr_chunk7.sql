-- Source: wf_DI_ITEM_MTRL_MASTER.XML
{{ config(
    materialized='view',
    tags=['staging', 'material_master']
) }}

with source_data as (
    select
        -- Core identifiers
        product,
        matergrppckgmater,
        productgroup,
        
        -- Units and measurements
        weightunit,
        volumeunit,
        materialvolume,
        baseunit,
        productmeasurementunit,
        
        -- Physical attributes
        netweight,
        grossweight,
        sizeordimensiontext,
        unitspecificproductlength,
        unitspecificproductwidth,
        unitspecificproductheight,
        
        -- Product hierarchy and classification
        producthierarchy,
        productoldid,
        itemcategorygroup,
        externalproductgroup,
        division,
        producttype,
        industrysector,
        
        -- Status and validity
        crossplantstatusvaliditydate,
        crossplantstatus,
        ismarkedfordeletion,
        maintainancestatus,
        
        -- Identifiers and codes
        productstandardid,
        internationalarticlenumbercat,
        authorizationgroup,
        
        -- Document management
        pgenumdocwithodocmanasys,
        industrystandardname,
        docverwithodocmanasys,
        doctypwithodocmanasys,
        docchngenumwithodocumanasys,
        docnumwithodocmanagsys,
        
        -- Manufacturing and design
        cadindicator,
        basicmaterial,
        pgeformatproducmemo,
        labdsgnoffc,
        
        -- Material properties
        indiinbulkliquid,
        indihighlyviscous,
        envrmtrelevant,
        packcode,
        
        -- Batch and quality management
        approvedbatchrecreg,
        isbatchmanagementrequired,
        ttlshelflife,
        temppcondindi,
        strgperc,
        strcond,
        roundrlcalsled,
        minremshelf,
        
        -- Safety and compliance
        hazardmatnum,
        contnrreq,
        prdindiexpdt,
        
        -- Labeling
        labeltype,
        labelform,
        serialnoexplicitnessLevel,
        
        -- Procurement
        manufacturernumber,
        varblpurordunitisactive,
        purchngvalkey,
        materqualdisckind,
        purchaseorderquantityunit,
        manufacturerpartprofile,
        productmanufacturernumber,
        materialfrieghtgroup,
        
        -- Logistics
        quantgrgiprnt,
        
        -- Descriptive
        productdescription,
        language,
        
        -- Origin and regulatory
        commodity,
        countryoforiginmaterial,
        
        -- Audit fields
        createdbyuser,
        timeofcreation,
        
        -- ETL metadata
        current_timestamp as etl_load_dt
        
    from {{ source('xml_source', 'xml_md_material_attr_txt') }}
    where product is not null
)

select * from source_data