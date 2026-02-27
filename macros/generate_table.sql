{% macro generate_pipeline() %}

    {% set meta = var('pipeline_metadata') %}

    {% set base_table = meta['base_table'] %}
    {% set base_alias = meta['base_alias'] %}
    {% set base_columns = meta['base_columns'] %}
    {% set joins = meta.get('joins', []) %}
    {% set where_clause = meta.get('where_clause') %}



    {% set select_columns = [] %}

    {# Base table columns #}
    {% for col in base_columns %}
        {% do select_columns.append(base_alias ~ '.' ~ col) %}
    {% endfor %}

    {# Join table columns #}
    {% for join in joins %}
        {% for col in join['columns'] %}
            {% do select_columns.append(
                join['alias'] ~ '.' ~ col ~
                ' AS ' ~ join['alias'] ~ '_' ~ col
            ) %}
        {% endfor %}
    {% endfor %}

    SELECT
        {{ select_columns | join(',\n        ') }}

    FROM {{ base_table }} AS {{ base_alias }}

    {% for join in joins %}
        LEFT JOIN {{ join['table'] }} AS {{ join['alias'] }}
            ON {{ join['join_condition'] }}
    {% endfor %}

    {% if where_clause %}
        WHERE {{ where_clause }}
    {% endif %}

{% endmacro %}