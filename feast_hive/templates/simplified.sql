{#
entity_dataframe: for each extractable entity set set the following fields:
- its composite key (full entity id)
- timestamp
- and for each extractable Feature View we set a special key string:
    {featureview}_entity_row_unique_id. It is composed from following string representation
    - all entity ids used in this FeatureView
    - timestamps of extractable entity (not feature view's timestamp = )
#}

WITH entity_dataframe AS
(
    SELECT *, {{entity_df_event_timestamp_col}} AS entity_timestamp{% if featureviews|length > 0 %},{% endif %}
        {%- for fv in featureviews %}
            CONCAT(
                {%- for entity in fv.entities %}CAST({{entity}} AS STRING), {% endfor -%}
                CAST({{entity_df_event_timestamp_col}} AS STRING)) AS {{ fv.name }}__entity_row_unique_id
                {%- if not loop.last %},{% endif -%}
        {%- endfor %}
    FROM {{ left_table_query_string }}
),
{#
Next, for each feature view we create a temp table {feature_view}__outputs.
We put here all data that we managed to extract from this feature view by given entities.

This table {feature_view}__output is created in following stages:

1) {feature_view}__entity_dataframe: a piece of entity_dataframe table,
that contains row unique id columns only for given {feature view}

2) {feature_view}__suitable_objects: IDs of objects in this feature view that match given entity
    (without any filtering by timestamps):
- fields of composite entity key for given feature view (NB: just for this current feature view!)
- entity_timestamp: a timestamp from entity_dataframe: we'll
    extract objects with date not later than this (and this field will take part in final JOIN)
- max_event_timestamp: the latest event timestamps aggregated by all objects found in featureview's table,
    when an object corresponds given entity key, but has ts <= than entity_timestamp (and >= entity_timestamp - TTL)
    If for some entity ID we cannot find such object, there will be just one row less in this table = )

3) {feature_view}__prefinal: And now let's take the previous table with keys and max timestamps of found objects
    and enrich it with the features from source feauture tables.

    NB: if in our source feature tables we have multiple rows with same entity key and event timestamp
        (equal to max timestamp) -- we can have duplicated rows here. To deduplicate them on later step
        we've added a special row__duplicate__index field calculated by ROW_NUMBER window function.

4) {feature_view}__output: here we took only deduplicated data (with row__duplicate__index == 1)
#}

{%- for fv in featureviews %}
{{ fv.name }}__entity_dataframe AS
(
    SELECT {{ fv.entities | join(', ')}}, entity_timestamp, {{ fv.name }}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ fv.entities | join(', ')}}, entity_timestamp, {{ fv.name }}__entity_row_unique_id
),
{{ fv.name }}__suitable_objects AS
(
    SELECT {{ fv.name }}__entity_dataframe.{{ fv.name }}__entity_row_unique_id,
        {% for entity in fv.entities %}{{ fv.table_subquery }}.{{ entity }},{% endfor %} MAX({{ fv.table_subquery }}.{{ fv.event_timestamp_column }}) AS max_event_timestamp
    FROM {{ fv.name }}__entity_dataframe INNER JOIN {{ fv.table_subquery }} ON
        {%- for entity in fv.entities %}
            {% if not loop.first %}AND {% endif %}{{ fv.name }}__entity_dataframe.{{ entity }} = {{ fv.table_subquery }}.{{ entity }}
        {% endfor -%}
    WHERE {{ fv.table_subquery }}.{{ fv.event_timestamp_column }} <= {{ fv.name }}__entity_dataframe.entity_timestamp
        AND {{ fv.table_subquery }}.{{ fv.event_timestamp_column }} >= {{ fv.name }}__entity_dataframe.entity_timestamp - interval {{ fv.ttl }} second
    GROUP BY {% for entity in fv.entities %}{{ fv.table_subquery }}.{{ entity }}, {% endfor %}{{ fv.name }}__entity_dataframe.{{ fv.name }}__entity_row_unique_id
),
{{ fv.name }}__prefinal AS
(
    SELECT
        {{ fv.name }}__suitable_objects.{{ fv.name }}__entity_row_unique_id,
        {%- for feature in fv.features %}
        {{ fv.table_subquery }}.{{ feature }} AS {{ feature }},
        {%- endfor %}
        ROW_NUMBER() OVER(PARTITION BY {{ fv.name }}__suitable_objects.{{ fv.name }}__entity_row_unique_id
            ORDER BY {{ fv.table_subquery }}.event_timestamp) AS {{ fv.name }}__duplicate_index
    FROM {{ fv.name }}__suitable_objects INNER JOIN {{ fv.table_subquery }}
    ON
        {%- for entity in fv.entities %}
        {% if not loop.first %}AND {% endif %}{{ fv.name }}__suitable_objects.{{ entity }} = {{ fv.table_subquery }}.{{ entity }}
        {% endfor -%}
        AND {{ fv.name }}__suitable_objects.max_event_timestamp = {{ fv.table_subquery }}.{{ fv.event_timestamp_column }}
),
{{ fv.name }}__output AS
(
    SELECT * FROM {{ fv.name }}__prefinal WHERE {{ fv.name }}__prefinal.{{ fv.name }}__duplicate_index = 1
){% if not loop.last %},{% endif %}
{%- endfor %}

{#
And here we do our big and fat final JOIN: each row of entity_dataframe is enriched with data found in
    feature views by its keys (or None, NaN and NaTs are written if no such data were found)
#}
SELECT
    {% for entity_key in unique_entity_keys %}entity_dataframe.{{ entity_key }}, {% endfor -%}
entity_dataframe.entity_timestamp AS {{ entity_df_event_timestamp_col }},
    {%- for fv in featureviews -%}
    {%- set outer_loop = loop -%}
    {%- for feature in fv.features %}
    {{ fv.name }}__output.{{ feature }} AS {% if full_feature_names %}{{ fv.name }}__{% endif -%}{{ feature }}
    {%- if not (loop.last and outer_loop.last) %},{% endif %}
    {%- endfor -%}
{%- endfor %}
FROM entity_dataframe
{%- for fv in featureviews %}
    LEFT OUTER JOIN {{ fv.name }}__output
        ON entity_dataframe.{{ fv.name }}__entity_row_unique_id = {{ fv.name }}__output.{{ fv.name }}__entity_row_unique_id
    {% endfor %}
