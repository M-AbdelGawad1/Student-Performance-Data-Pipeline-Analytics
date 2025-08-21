{{ config(
    materialized='table',
    schema='gold_layer'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_student') }}
),

unique_students AS (
    SELECT DISTINCT
        age,
        gender,
        major,
        semester,
        family_income,
        PARENTS_EDUCATION,
    FROM silver_data
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY age, gender, major, semester) as id,  -- Primary Key
    age,
    gender,
    major,
    semester,
    family_income,
    PARENTS_EDUCATION
FROM unique_students