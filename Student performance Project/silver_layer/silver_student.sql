{{ config(
    materialized='view',
    schema='silver'
) }}

WITH source AS (
    SELECT * 
    FROM {{ source('student_raw', 'student_performance') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY student_id) AS student_id,
    
    COALESCE(TRIM(major), 'Undeclared') as major,
    
    
    CASE 
        WHEN LOWER(gender) = 'other' THEN 'Prefer not to say'
        WHEN gender IS NULL THEN 'Unknown'
        ELSE LOWER(gender)
    END AS gender, 
    
    CASE 
        WHEN age BETWEEN 10 AND 25 THEN age 
        ELSE -1 
    END AS age,
    
    CASE 
        WHEN study_hours_per_day BETWEEN 0 AND 24 THEN ROUND(study_hours_per_day,1) 
        ELSE 0 
    END AS study_hours,
    
    
    CASE 
        WHEN social_media_hours BETWEEN 0 AND 24 THEN ROUND(social_media_hours,2)     
        ELSE 0 
    END AS social_media_hours,
    
    CASE 
        WHEN netflix_hours BETWEEN 0 AND 24 THEN ROUND(netflix_hours,2)     
        ELSE 0 
    END AS netflix_hours,
    
    CASE 
        WHEN sleep_hours BETWEEN 0 AND 24 THEN ROUND(sleep_hours,2)     
        ELSE 8.0 
    END AS sleep_hours,
    
    
    CASE 
        WHEN exam_score BETWEEN 0 AND 100 THEN exam_score 
        ELSE 0 
    END AS exam_score,
    
    CASE 
        WHEN exercise_frequency BETWEEN 0 AND 7 THEN exercise_frequency   
        ELSE 0 
    END AS exercise_frequency_weekly,
    
    COALESCE(LOWER(TRIM(diet_quality)), 'fair') AS diet_quality,
    
    
    COALESCE(TRIM(parental_education_level), 'High school') AS parents_education,
    
    COALESCE(TRIM(internet_quality), 'Medium') AS internet_quality,
    
    COALESCE(mental_health_rating, 5) AS mental_health_rating,
    
    COALESCE(extracurricular_participation, 'No') AS extracurricular_participation,
    
    CASE 
        WHEN previous_gpa BETWEEN 0 AND 4 THEN ROUND(previous_gpa,2) 
        ELSE 0.0 
    END AS gpa,
    
    COALESCE(TRIM(semester), -1) AS semester,
    
    CASE 
        WHEN stress_level BETWEEN 0 AND 10 THEN ROUND(stress_level,1) 
        ELSE 5.0 
    END AS stress_level,
    
    COALESCE(dropout_risk, 'No') AS dropout_risk, 
    
    CASE 
        WHEN social_activity BETWEEN 0 AND 5 THEN social_activity 
        ELSE 2 
    END AS social_activity,
    
    COALESCE(TRIM(study_environment), 'Unknown') AS study_environment,
    
    
    COALESCE(TRIM(family_income_range), 'Not Disclosed') AS family_income,
    
    CASE 
        WHEN parental_support_level BETWEEN 0 AND 10 THEN parental_support_level 
        ELSE 5 
    END AS parental_support_level,
    
    
    CASE 
        WHEN motivation_level BETWEEN 0 AND 10 THEN motivation_level 
        ELSE 5 
    END AS motivation_level,
    
    CASE 
        WHEN exam_anxiety_score BETWEEN 0 AND 10 THEN exam_anxiety_score 
        ELSE 5 
    END AS exam_anxiety_score,
    
    CASE 
        WHEN ATTENDANCE_PERCENTAGE BETWEEN 0 AND 100 THEN ATTENDANCE_PERCENTAGE 
        ELSE -1
    END AS attendance_percentage,
    
    
    COALESCE(TRIM(learning_style), 'Unknown') AS learning_style

FROM source