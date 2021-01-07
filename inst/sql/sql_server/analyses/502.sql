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


--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData 
as
(
  select
    YEAR(d1.death_date)*100 + month(d1.death_date) as stratum_1,
    COUNT_BIG(distinct d1.PERSON_ID) as count_value
  from @cdmDatabaseSchema.death d1
  join @cdmDatabaseSchema.observation_period op on d1.person_id = op.person_id
  where d1.death_date <= op.observation_period_end_date
    and d1.death_date >= op.observation_period_start_date
  group by YEAR(d1.death_date)*100 + month(d1.death_date)
)
SELECT
  @analysisId as analysis_id,
  CAST(stratum_1 AS VARCHAR(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
FROM rawData;