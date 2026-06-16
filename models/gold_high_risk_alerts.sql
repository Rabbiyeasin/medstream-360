{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    subject_id,
    is_abnormal,
    clinical_alert,
    DATE(charttime) AS observation_date
FROM {{ ref('silver_vitals') }}
WHERE is_abnormal = TRUE