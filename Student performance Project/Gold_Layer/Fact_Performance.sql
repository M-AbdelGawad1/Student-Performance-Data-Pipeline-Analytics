{{ config(
    materialized='table',
    schema='gold_layer'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_student') }}
),


student_dim AS (
    SELECT * FROM {{ ref('Dim_Student') }}
),

psychological_dim AS (
    SELECT * FROM {{ ref('Dim_psychological') }}
),

lifestyle_dim AS (
    SELECT * FROM {{ ref('Dim_lifeStyle') }}
)

SELECT 
    
    
sd.id as student_fk,
    pd.psy_identifier as psy_identifier_fk,
    ld.identifier as lifestyle_id_fk,
    
    -- Measures
    s.study_hours,
    s.exam_score,
    s.attendance_percentage,
    s.gpa
    
FROM silver_data s

LEFT JOIN student_dim sd ON 
    s.age = sd.age 
    AND s.gender = sd.gender 
    AND s.major = sd.major 
    AND s.semester = sd.semester
    AND s.family_income = sd.family_income
    AND s.parents_education = sd.parents_education

LEFT JOIN psychological_dim pd ON 
    s.mental_health_rating = pd.mental_health_rating
    AND s.stress_level = pd.stress_level
    AND s.motivation_level = pd.motivation_level
    AND s.social_activity = pd.social_activity
    AND s.exam_anxiety_score = pd.exam_anxiety

LEFT JOIN lifestyle_dim ld ON 
    s.diet_quality = ld.diet_quality
    AND s.learning_style = ld.learning_style
    AND s.study_environment = ld.study_environment
    AND s.exercise_frequency_weekly = ld.exercise_frequency
    AND s.sleep_hours = ld.sleep_hours
    AND s.netflix_hours = ld.netflix_hours
    AND s.social_media_hours = ld.social_media_hours
    AND s.internet_quality = ld.internet_quality