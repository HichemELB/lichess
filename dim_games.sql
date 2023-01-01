{{config(
    materialized = 'table'
)}}

SELECT
    Site_id AS game_id
    , CAST(CAST(DATE(UTCDate) AS STRING) || ' ' || CAST(UTCTime AS STRING) AS TIMESTAMP) AS game_utc_at
    , White AS white_user_id
    , Black AS black_user_id
    , CAST(REPLACE(REGEXP_EXTRACT(TimeControl, '.*[+]'), '+', '') AS INT64) * 2 AS play_time
    , TimeControl AS time_control
FROM {{ ref('game_header') }}