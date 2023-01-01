{{
    config(
        materialized = 'table'
)}}

    SELECT
        Site_id,
        Date,
        CAST(CAST(DATE(game_header.UTCDate) AS STRING) || ' ' || CAST(game_header.UTCTime AS STRING) AS TIMESTAMP) AS UTCDateTime,
        Site,
        Event,
        White,
        Black,
        Result,
        BlackElo,
        BlackRatingDiff,
        ECO,
        Opening,
        Termination,
        TimeControl,
        WhiteElo,
        WhiteRatingDiff,
        BlackTitle,
        Round,
        WhiteTitle,
        CAST(REPLACE(REGEXP_EXTRACT(TimeControl, '.*[+]'), '+', '') AS INT64) * 2 AS aprox_play_time
    FROM
        {{ source('dbt_silver', 'game_header') }}