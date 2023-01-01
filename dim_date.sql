{{config(
    materialized = 'table'
)}}

SELECT
  d as date,
  FORMAT_DATE('%Y-%m', d) as yearMonth,
  EXTRACT(DAY FROM d) as dayOfMonth,
  FORMAT_DATE('%w', d) AS dayOfWeek, --starting 0sunday
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2021-11-01', '2023-01-01', INTERVAL 1 DAY)) AS d )
