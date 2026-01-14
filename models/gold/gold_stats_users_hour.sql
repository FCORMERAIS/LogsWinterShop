WITH base AS (
    SELECT *
    FROM {{ ref('gold_visit_per_hour') }}
)
 
SELECT
    MIN(visitors_count) AS min_visitors,
    MAX(visitors_count) AS max_visitors,
    AVG(visitors_count) AS avg_visitors
FROM base