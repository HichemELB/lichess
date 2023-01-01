{{config(
    materialized = 'table'
)}}

SELECT
    Site_id AS game_id
    , UTCDateTime AS game_utc_at
    , White AS white_user_id
    , Black AS black_user_id
    , aprox_play_time AS play_time
    , TimeControl AS time_control
FROM {{ ref('game_header_cleaned') }}