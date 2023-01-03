{{config(
    materialized = 'table'
)}}

WITH
  user_activity AS (
  SELECT
    DISTINCT DATE_TRUNC(`date`, day) AS game_day_at,
    DATE_TRUNC(`date`, week) AS game_week_at,
    DATE_TRUNC(`date`, month) AS game_month_at,
    intial_game_utc_at,
    user_id
  FROM
    {{ ref('fact_user_game') }}),
  one_day_retention AS (
  SELECT
    DATE_TRUNC(previous.intial_game_utc_at, day) AS intial_game_day_at,
    ROUND(COUNT(DISTINCT `current`.user_id) * 100 / GREATEST(COUNT(DISTINCT previous.user_id), 1), 2) AS one_day_retention_rate
  FROM
    user_activity AS previous
  LEFT JOIN
    user_activity AS `current`
  ON
    `current`.user_id = previous.user_id
    AND DATE_TRUNC(previous.intial_game_utc_at, day) = DATE_SUB(`current`.game_day_at, INTERVAL 1 day )
  GROUP BY
    1
  ORDER BY
    1 ASC),
  one_week_retention AS (
  SELECT
    DATE_TRUNC(previous.intial_game_utc_at, week) AS intial_game_week_at,
    ROUND(COUNT(DISTINCT `current`.user_id) * 100 / GREATEST(COUNT(DISTINCT previous.user_id), 1), 2) AS one_week_retention_rate
  FROM
    user_activity AS previous
  LEFT JOIN
    user_activity AS `current`
  ON
    `current`.user_id = previous.user_id
    AND DATE_TRUNC(previous.intial_game_utc_at, week) = DATE_SUB(`current`.game_week_at, INTERVAL 1 week )
  GROUP BY
    1
  ORDER BY
    1 ASC),
  one_month_retention AS (
  SELECT
    DATE_TRUNC(previous.intial_game_utc_at, month) AS intial_game_month_at,
    ROUND(COUNT(DISTINCT `current`.user_id) * 100 / GREATEST(COUNT(DISTINCT previous.user_id), 1), 2) AS one_month_retention_rate
  FROM
    user_activity AS previous
  LEFT JOIN
    user_activity AS `current`
  ON
    `current`.user_id = previous.user_id
    AND DATE_TRUNC(previous.intial_game_utc_at, month) = DATE_SUB(`current`.game_month_at, INTERVAL 1 month )
  GROUP BY
    1
  ORDER BY
    1 ASC)
SELECT
  `date`,
  one_day.one_day_retention_rate,
  one_week.one_week_retention_rate,
  one_month.one_month_retention_rate,
FROM
  {{ ref('dim_date') }} d
LEFT JOIN
  one_day_retention one_day
ON
  d.`date` = one_day.intial_game_day_at
LEFT JOIN
  one_week_retention one_week
ON
  DATE_TRUNC(d.`date`, week) = one_week.intial_game_week_at
LEFT JOIN
  one_month_retention one_month
ON
  DATE_TRUNC(d.`date`, month) = one_month.intial_game_month_at