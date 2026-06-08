{#
  generate_surrogate_key: deterministic hash key from a list of columns.
  Mirrors dbt_utils.generate_surrogate_key but kept local (no extra package
  needed). Coalesces NULLs to a sentinel so the hash is stable.
#}
{% macro generate_surrogate_key(field_list) -%}
    cityHash64(
        {%- for field in field_list %}
            toString(coalesce(toString({{ field }}), '_null_'))
            {%- if not loop.last %} , '|' , {% endif -%}
        {%- endfor %}
    )
{%- endmacro %}
