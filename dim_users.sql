{{config(
    materialized = 'table'
)}}


WITH white_black AS (
    SELECT White AS user_id FROM {{ ref('game_header') }}
    UNION ALL
    SELECT Black AS user_id FROM {{ ref('game_header') }}
),
    users AS (
    SELECT DISTINCT user_id
    FROM white_black
),
    initial_game AS (
    SELECT
        users.user_id
      , MIN(CAST(CAST(DATE(game_header.UTCDate) AS STRING) || ' ' || CAST(game_header.UTCTime AS STRING) AS TIMESTAMP)) OVER (PARTITION BY game_header.White) AS intial_game_utc_at
    FROM users
    INNER JOIN {{ ref('game_header') }} game_header
        ON users.user_id = game_header.White
    UNION ALL
    SELECT
        users.user_id
       , MIN(CAST(CAST(DATE(game_header.UTCDate) AS STRING) || ' ' || CAST(game_header.UTCTime AS STRING) AS TIMESTAMP)) OVER (PARTITION BY game_header.Black) AS intial_game_utc_at
    FROM users
    INNER JOIN {{ ref('game_header') }} game_header
        ON users.user_id = game_header.Black
)
SELECT
    user_id,
    intial_game_utc_at
FROM initial_game
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY intial_game_utc_at ASC) = 1
