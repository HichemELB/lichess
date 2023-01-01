{{config(
    materialized = 'table'
)}}


WITH white_black AS (
    SELECT White AS user_id FROM {{ ref('game_header_cleaned') }}
    UNION ALL
    SELECT Black AS user_id FROM {{ ref('game_header_cleaned') }}
),
    users AS (
    SELECT DISTINCT user_id
    FROM white_black
),
    initial_game AS (
    SELECT
        users.user_id
      , MIN(UTCDateTime) OVER (PARTITION BY game_header.White) AS intial_game_utc_at
    FROM users
    INNER JOIN {{ ref('game_header_cleaned') }} game_header
        ON users.user_id = game_header.White
    UNION ALL
    SELECT
        users.user_id
       , MIN(UTCDateTime) OVER (PARTITION BY game_header.Black) AS intial_game_utc_at
    FROM users
    INNER JOIN {{ ref('game_header_cleaned') }} game_header
        ON users.user_id = game_header.Black
)
SELECT
    user_id,
    intial_game_utc_at
FROM initial_game
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY intial_game_utc_at ASC) = 1
