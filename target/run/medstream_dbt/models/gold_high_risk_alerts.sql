
  
    
    

    create  table
      "dev"."main_gold"."gold_high_risk_alerts__dbt_tmp"
  
    as (
      

SELECT 
    subject_id,
    charttime,
    vital_type,
    value,
    is_abnormal,
    CASE 
        WHEN vital_type = 'glucose' AND value > 200 THEN 'High Glucose - Readmission Risk'
        WHEN vital_type = 'heart_rate' AND value > 120 THEN 'Tachycardia - Readmission Risk'
        WHEN vital_type = 'respiratory_rate' AND (value < 8 OR value > 30) THEN 'Respiratory Distress'
        ELSE 'Monitor'
    END as clinical_alert
FROM "dev"."main_silver"."silver_vitals"
WHERE is_abnormal = 1
ORDER BY charttime DESC
    );
  
  