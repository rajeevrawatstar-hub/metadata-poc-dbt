{% macro generate_table(model_name) %}

{% set query %}
select *
from {{ ref('pipeline_metadata') }}
where model_name = '{{ model_name }}'
{% endset %}

{% set results = run_query(query) %}

{% if execute %}

{% set rows = results.rows %}

{% set base_table = rows[0][1] %}
{% set base_alias = rows[0][2] %}
{% set where_clause = rows[0][9] %}

{% set base_columns = [] %}
{% set joins = {} %}

{% for row in rows %}

    {% set base_column = row[3] %}
    {% set join_table = row[4] %}
    {% set join_alias = row[5] %}
    {% set join_type = row[6] %}
    {% set join_condition = row[7] %}
    {% set join_column = row[8] %}

    {% if base_column %}
        {% do base_columns.append(base_alias ~ '.' ~ base_column) %}
    {% endif %}

    {% if join_table %}
    
        {% set key = join_table ~ join_alias %}
        
        {% if key not in joins %}
            {% do joins.update({
                key: {
                    "table": join_table,
                    "alias": join_alias,
                    "type": join_type,
                    "condition": join_condition,
                    "columns": []
                }
            }) %}
        {% endif %}

        {% do joins[key]["columns"].append(join_column) %}

    {% endif %}

{% endfor %}

SELECT

{% for col in base_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}

{% for join in joins.values() %}
{% for col in join.columns %}
    ,{{ join.alias }}.{{ col }} AS {{ join.alias }}_{{ col }}
{% endfor %}
{% endfor %}

FROM {{ base_table }} AS {{ base_alias }}

{% for join in joins.values() %}
{{ join.type }} JOIN {{ join.table }} AS {{ join.alias }}
ON {{ join.condition }}
{% endfor %}

{% if where_clause %}
WHERE {{ where_clause }}
{% endif %}

{% endif %}

{% endmacro %}