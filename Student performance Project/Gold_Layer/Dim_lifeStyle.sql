{{ config(
    materialized='table',
    schema='gold_layer'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_student') }}
),

unique_lifestyle AS (
    SELECT DISTINCT
        diet_quality,
        learning_style,
        exercise_frequency_weekly,
        study_environment,
        sleep_hours,
        netflix_hours,
        social_media_hours,
        internet_quality
    FROM silver_data
    WHERE diet_quality IS NOT NULL
      AND learning_style IS NOT NULL
      AND study_environment IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY diet_quality, learning_style, study_environment) as identifier,  -- Primary Key
    diet_quality,
    learning_style,
    exercise_frequency_weekly as exercise_frequency,
    study_environment,
    sleep_hours,
    netflix_hours,
    social_media_hours,
    internet_quality
FROM unique_lifestyle
