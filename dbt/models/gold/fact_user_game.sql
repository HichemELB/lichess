{{config(
    materialized = 'table'
)}}

WITH game_user as (
    SELECT
        DATE(dim_games.game_utc_at) AS `date`
        , dim_users.user_id
        , DATE(dim_users.intial_game_utc_at) AS intial_game_utc_at
        , dim_games.game_id
        , dim_games.play_time
        --, COUNT(dim_games.game_id) AS played_games_count
        --, sum(dim_games.play_time) AS played_time_total
    FROM {{ ref("dim_users") }} dim_users
    INNER JOIN {{ ref("dim_games") }} dim_games
        ON dim_users.user_id = dim_games.white_user_id
    --GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        DATE(dim_games.game_utc_at) AS `date`
        , dim_users.user_id
        , DATE(dim_users.intial_game_utc_at) AS intial_game_utc_at
        , dim_games.game_id
        , dim_games.play_time
        --, COUNT(dim_games.game_id) AS played_games_count
        --, sum(dim_games.play_time) AS played_time_total
    FROM {{ ref("dim_users") }} dim_users
    INNER JOIN {{ ref("dim_games") }} dim_games
        ON dim_users.user_id = dim_games.black_user_id
    --GROUP BY 1, 2, 3
)
SELECT
    *
FROM game_user