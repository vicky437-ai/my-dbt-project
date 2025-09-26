-- Macro for material characteristic lookups
-- Reusable macro to standardize lookup logic across different characteristics

{% macro lookup_material_characteristic(characteristic_name, return_column='char_value') %}
    (
        select {{ return_column }}
        from {{ source('sap_raw', 'material_characteristics') }}
        where characteristic_name = '{{ characteristic_name }}'
            and key_of_obj = material.product
        limit 1  -- Use first value on multiple match
    )
{% endmacro %}