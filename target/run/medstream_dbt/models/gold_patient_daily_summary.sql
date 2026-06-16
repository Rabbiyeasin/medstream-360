
  
    
    

    create  table
      "dev"."main_gold"."gold_patient_daily_summary__dbt_tmp"
  
    as (
      

SELECT 
    subject_id,
    observation_date,
    vital_type,
    AVG(value) as avg_value,
    MAX(value) as max_value,
    MIN(value) as min_value,
    COUNT(*) as measurement_count,
    MAX(CASE WHEN is_abnormal = 1 THEN 1 ELSE 0 END) as had_abnormal_event
FROM "dev"."main_silver"."silver_vitals"
GROUP BY subject_id, observation_date, vital_type
    );
  
  