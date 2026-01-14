WITH parsed AS (
    SELECT
        log_line,
 
        -- IP
        split_part(log_line, ' ', 1) AS ip_address,
 
        -- Date
        to_timestamp(
            regexp_replace(
                regexp_substr(log_line, '\\[(.*?)\\]'), 
                '\\[|\\]', ''
            ), 
            'DD/Mon/YYYY:HH24:MI:SS'
        ) AS log_date,
 
        -- Méthode HTTP
        regexp_substr(log_line, '"(GET|POST|PUT|DELETE|PATCH)') AS http_method,
 
        -- URL
        regexp_substr(log_line, '"(?:GET|POST|PUT|DELETE|PATCH) (.*?) HTTP/') AS url_path,
 
        -- Code HTTP
        split_part(
            regexp_substr(log_line, '" (\\d{3}) '), 
            ' ', 2
        )::int AS status_code,
 
        -- Taille de la réponse
        split_part(
            regexp_substr(log_line, '" \\d{3} (\\d+)'), 
            ' ', 2
        )::int AS response_size,
 
        -- Referrer
        regexp_substr(log_line, '"https?://[^"]*"', 1, 2) AS referrer,
 
        -- User agent
        regexp_substr(log_line, '".*"$') AS user_agent
 
    FROM {{ source('prod', 'bronze_logs') }}
)
 
SELECT *
FROM parsed