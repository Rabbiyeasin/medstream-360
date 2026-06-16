{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    storetime,
    itemid,
    value,
    valueuom,
    ingestion_time,

    -- 🔥 derived column: vital type
    CASE 
        WHEN itemid = 220045 THEN 'heart_rate'
        WHEN itemid = 225664 THEN 'blood_pressure'
        WHEN itemid = 220210 THEN 'respiratory_rate'
        WHEN itemid = 220277 THEN 'oxygen_saturation'
        ELSE 'other'
    END AS vital_type,

    -- 🔥 derived column: abnormal flag
    CASE 
        WHEN itemid = 220045 AND value > 100 THEN TRUE
        WHEN itemid = 220045 AND value < 50 THEN TRUE

        WHEN itemid = 225664 AND value > 140 THEN TRUE

        WHEN itemid = 220277 AND value < 92 THEN TRUE

        ELSE FALSE
    END AS is_abnormal,

    -- date feature
    DATE(charttime) as observation_date

FROM {{ source('medstream_360', 'bronze_vitals') }}