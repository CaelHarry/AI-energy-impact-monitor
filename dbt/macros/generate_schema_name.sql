{% macro generate_schema_name(custom_schema_name, node) -%}

    {#-
        Override dbt's default schema naming so that +schema: staging produces
        the schema "staging" exactly — not "marts_staging" (the default prefix
        behaviour concatenates the profile's target schema with the custom one).
    -#}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
