WITH parsed AS (
    SELECT
        -- Identifiant et date d'intégration depuis la table Bronze
        id AS bronze_id,
        ingested_at,
 
        -- IP
        split_part(log_line, ' ', 1) AS ip_address,
 
        -- Date du log
        to_timestamp(
            regexp_replace(
                split_part(log_line, '[', 2),
                '\\].*', ''
            ),
            'DD/Mon/YYYY:HH24:MI:SS'
        ) AS log_date,
 
        -- Méthode HTTP
        split_part(split_part(log_line, '"', 2), ' ', 1) AS http_method,
 
        -- URL demandée
        split_part(split_part(log_line, '"', 2), ' ', 2) AS url_path,
 
        -- Code HTTP
        split_part(split_part(log_line, '"', 3), ' ', 2)::int AS status_code,
 
        -- Taille de la réponse
        split_part(split_part(log_line, '"', 3), ' ', 3)::int AS response_size,
 
        -- Referrer
        split_part(split_part(log_line, '"', 4), '"', 1) AS referrer,
 
        -- User agent
        split_part(split_part(log_line, '"', 6), '"', 1) AS user_agent
 
    FROM {{ source('prod', 'bronze_logs') }}
)
 
SELECT *
FROM parsed