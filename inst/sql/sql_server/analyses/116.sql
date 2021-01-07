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


select distinct 
  YEAR(observation_period_start_date) as obs_year 
INTO #temp_dates_@analysisId
from @cdmDatabaseSchema.observation_period
;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData
as
(
  select
    t1.obs_year as stratum_1,
    p1.gender_concept_id as stratum_2,
    floor((t1.obs_year - p1.year_of_birth)/10) as stratum_3,
    COUNT_BIG(distinct p1.PERSON_ID) as count_value
  from @cdmDatabaseSchema.person p1
  join @cdmDatabaseSchema.observation_period op1 on p1.person_id = op1.person_id
  cross join #temp_dates_@analysisId t1
  where year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
    and year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
  group by t1.obs_year,
    p1.gender_concept_id,
    floor((t1.obs_year - p1.year_of_birth)/10)
)
SELECT
  @analysisId as analysis_id,
  CAST(stratum_1 AS VARCHAR(255)) as stratum_1,
  cast(stratum_2 as varchar(255)) as stratum_2,
  cast(stratum_3 as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
FROM rawData;

TRUNCATE TABLE #temp_dates_@analysisId;
DROP TABLE #temp_dates_@analysisId;
