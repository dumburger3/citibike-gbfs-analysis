{#
  By default dbt names custom schemas "<target schema>_<custom>", which would
  give STAGING_MARTS. Override so +schema: marts builds into exactly MARTS.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
