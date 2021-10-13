-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

--HINT DISTRIBUTE_ON_KEY(stratum_1)

CREATE TABLE TEMP_110 AS
SELECT DISTINCT 
    EXTRACT(YEAR FROM observation_period_start_date)*100 + EXTRACT(MONTH FROM observation_period_start_date) AS obs_month,
    TO_DATE(TO_CHAR(EXTRACT(YEAR FROM observation_period_start_date),'0000')||'-'||TO_CHAR(EXTRACT(MONTH FROM observation_period_start_date),'00')||'-'||TO_CHAR(1,'00'), 'YYYY-MM-DD')
    AS obs_month_start,
    TO_DATE(to_char(last_day(observation_period_start_date),'YYYY-MM-DD')||' 23:59:59','YYYY-MM-DD HH24:MI:SS') AS obs_month_end
  FROM OMOP.observation_period
  ORDER BY OBS_MONTH;

SELECT
  110 as analysis_id,  
	CAST(t1.obs_month AS VARCHAR(255)) as stratum_1,
	cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
	COUNT_BIG(distinct op1.PERSON_ID) as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_110
FROM
@cdmDatabaseSchema.observation_period op1
join 
TEMP_110 t1 on	op1.observation_period_start_date <= t1.obs_month_start
	and	op1.observation_period_end_date >= t1.obs_month_end
group by t1.obs_month;

DROP TABLE TEMP_110;





