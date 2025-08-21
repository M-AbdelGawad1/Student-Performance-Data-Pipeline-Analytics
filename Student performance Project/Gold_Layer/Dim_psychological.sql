{{ config(
    materialized='table',
    schema='gold_layer'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_student') }}
),

unique_psychological AS (
    SELECT DISTINCT
        mental_health_rating,
        stress_level,
        motivation_level,
        social_activity,
        exam_anxiety_score
    FROM silver_data
    
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY mental_health_rating, stress_level, motivation_level) as psy_identifier,  -- Primary Key
    mental_health_rating as mental_health_rating,
    stress_level as stress_level,
    motivation_level as motivation_level,
    social_activity,
    exam_anxiety_score as exam_anxiety
FROM unique_psychological
