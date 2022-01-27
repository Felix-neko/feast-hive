/*
This query is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
There are many changes compare to the BigQuery one:

1. Use "t - interval 'x' second" instead of "Timestamp_sub(...)".
2. Replace `SELECT * EXCEPT (...)` with `REGEX Column Specification`, because `EXCEPT` is not supported by Hive.
   Can study more here: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select.
3. Change `USING` to `ON`, since Hive doesn't support `USING`.
4. Change ANY_VALUE() to MAX(), since Hive doesn't support `ANY_VALUE`.
5. Change Subquery `SELECT MAX(entity_timestamp) FROM entity_dataframe` and `SELECT MIN(entity_timestamp) FROM
   entity_dataframe` to `INNER JOIN`. Since Hive does not support more than a single Subquery, and doesn't supports
   Subquery on non-top level.

6. Mostly big change here is that I tested the original Query on my own Hive set ups (3.0.0 and 3.1.2, based on Spark
   and Yarn), the CTE didn't work, `*__cleaned` had no result, but after I changed the CTE to Temporary Table it
   worked. Not sure if it was a common case, but here I changed it to the workable Temporary Table.
7. Move `(MIN(entity_timestamp) - interval 'ttl' second)` to SELECT clause, since Temporary Table
   does support it in Where clause.

In case there was any improvement on BigQuery, probably need to apply here too.
*/

/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE TEMPORARY TABLE entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
);


-- Start create temporary table *__base
{% for featureview in featureviews %}

CREATE TEMPORARY TABLE {{ featureview.name }}__base AS
WITH {{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}},
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
               {% if featureview.ttl == 0 %}{% else %}
               ,(MIN(entity_timestamp) - interval '{{ featureview.ttl }}' second) as min_entity_timestamp_
               {% endif %}
        FROM entity_dataframe
    ) AS temp
    ON (
        {{ featureview.event_timestamp_column }} <= max_entity_timestamp_
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.event_timestamp_column }} >=  min_entity_timestamp_
        {% endif %}
    )
)
SELECT
    subquery.*,
    entity_dataframe.entity_timestamp,
    entity_dataframe.{{featureview.name}}__entity_row_unique_id
FROM {{ featureview.name }}__subquery AS subquery
INNER JOIN (
    SELECT *
    {% if featureview.ttl == 0 %}{% else %}
    , (entity_timestamp - interval '{{ featureview.ttl }}' second) as ttl_entity_timestamp
    {% endif %}
    FROM {{ featureview.name }}__entity_dataframe
) AS entity_dataframe
ON (
    subquery.event_timestamp <= entity_dataframe.entity_timestamp

    {% if featureview.ttl == 0 %}{% else %}
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    {% endif %}

    {% for entity in featureview.entities %}
    AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
    {% endfor %}
);

{% endfor %}
-- End create temporary table *__base


{% for featureview in featureviews %}

{% if loop.first %}WITH{% endif %}

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        base.{{featureview.name}}__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX(base.created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base AS base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup AS dedup
        ON (
            dedup.{{featureview.name}}__entity_row_unique_id=base.{{featureview.name}}__entity_row_unique_id
            AND dedup.event_timestamp=base.event_timestamp
            AND dedup.created_timestamp=base.created_timestamp
        )
    {% endif %}

    GROUP BY base.{{featureview.name}}__entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base AS base
    INNER JOIN {{ featureview.name }}__latest AS latest
    ON (
        base.{{featureview.name}}__entity_row_unique_id=latest.{{featureview.name}}__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
        {% if featureview.created_timestamp_column %}
            AND base.created_timestamp=latest.created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}

/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT `(entity_timestamp|{% for featureview in featureviews %}{{featureview.name}}__entity_row_unique_id{% if loop.last %}{% else %}|{% endif %}{% endfor %})?+.+`
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) AS {{ featureview.name }}__joined
ON (
    {{ featureview.name }}__joined.{{featureview.name}}__entity_row_unique_id=entity_dataframe.{{featureview.name}}__entity_row_unique_id
)
{% endfor %}