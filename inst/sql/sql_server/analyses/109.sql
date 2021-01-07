/*********
Achilles Analysis #@analysisId:
- Analysis Name = @analysisName

Parameters used in this template:
- cdmDatabaseSchema = @cdmDatabaseSchema
- scratchDatabaseSchema = @scratchDatabaseSchema
- oracleTempSchema = @oracleTempSchema
- schemaDelim = @schemaDelim
- tempAchillesPrefix = @tempAchillesPrefix
**********/

-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

--HINT DISTRIBUTE_ON_KEY(obs_year)
SELECT DISTINCT 
  YEAR(observation_period_start_date) AS obs_year,
  DATEFROMPARTS(YEAR(observation_period_start_date), 1, 1) AS obs_year_start,
  DATEFROMPARTS(YEAR(observation_period_start_date), 12, 31) AS obs_year_end
INTO #temp_dates_@analysisId
FROM @cdmDatabaseSchema.observation_period
;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT 
  @analysisId AS analysis_id,  
	CAST(obs_year AS VARCHAR(255)) AS stratum_1,
	cast(null as varchar(255)) as stratum_2, 
	cast(null as varchar(255)) as stratum_3, 
	cast(null as varchar(255)) as stratum_4, 
	cast(null as varchar(255)) as stratum_5,
	COUNT_BIG(DISTINCT person_id) AS count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
FROM @cdmDatabaseSchema.observation_period cross join #temp_dates_@analysisId
WHERE	observation_period_start_date <= obs_year_start
	AND observation_period_end_date >= obs_year_end
GROUP BY obs_year
;

TRUNCATE TABLE #temp_dates_@analysisId;
DROP TABLE #temp_dates_@analysisId;
