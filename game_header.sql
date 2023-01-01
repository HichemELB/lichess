{{
    config(
        materialized = 'incremental',
        unique_key = 'Site_id',
        partition_by = { 'field': '`Date`', 'data_type': 'timestamp', 'granularity':'day' },
        incremental_strategy = 'merge',
)}}

WITH get_new_rows AS (
    SELECT
	    inserted_at,
	    row,
	    REPLACE(JSON_EXTRACT_SCALAR(row, '$.Site'), "https://lichess.org/", '') AS Site_id,
    FROM
        {{ source('lichess_org', 'dataflow__lichess__GameHeaderStream_*') }}
	{% if is_incremental() %}
	WHERE
        inserted_at > (SELECT max(inserted_at) FROM {{ this }})
	{% endif %}
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Site_id ORDER BY inserted_at DESC) = 1
)

SELECT
  inserted_at,
  REPLACE(JSON_EXTRACT_SCALAR(row, '$.Site'), "https://lichess.org/", '') AS Site_id,
  SAFE_CAST(REPLACE(JSON_EXTRACT_SCALAR(row, '$.Date'), '.', '-') AS TIMESTAMP) AS `Date`,
  JSON_EXTRACT_SCALAR(row, '$.Site') AS Site,
  JSON_EXTRACT_SCALAR(row, '$.Event') AS Event,
  JSON_EXTRACT_SCALAR(row, '$.White') AS White,
  JSON_EXTRACT_SCALAR(row, '$.Black') AS Black,
  JSON_EXTRACT_SCALAR(row, '$.Result') AS Result,
  SAFE_CAST(JSON_EXTRACT_SCALAR(row, '$.BlackElo') AS INTEGER) AS BlackElo,
  JSON_EXTRACT_SCALAR(row, '$.BlackRatingDiff') AS BlackRatingDiff,
  JSON_EXTRACT_SCALAR(row, '$.ECO') AS ECO,
  JSON_EXTRACT_SCALAR(row, '$.Opening') AS Opening,
  JSON_EXTRACT_SCALAR(row, '$.Termination') AS Termination,
  JSON_EXTRACT_SCALAR(row, '$.TimeControl') AS TimeControl,
  SAFE_CAST(REPLACE(JSON_EXTRACT_SCALAR(row, '$.UTCDate'), '.', '-') AS TIMESTAMP) AS UTCDate,
  JSON_EXTRACT_SCALAR(row, '$.UTCTime') AS UTCTime,
  SAFE_CAST(JSON_EXTRACT_SCALAR(row, '$.WhiteElo') AS INTEGER) AS WhiteElo,
  JSON_EXTRACT_SCALAR(row, '$.WhiteRatingDiff') AS WhiteRatingDiff,
  JSON_EXTRACT_SCALAR(row, '$.BlackTitle') AS BlackTitle,
  JSON_EXTRACT_SCALAR(row, '$.Round') AS `Round`,
  JSON_EXTRACT_SCALAR(row, '$.WhiteTitle') AS WhiteTitle
FROM
    get_new_rows
