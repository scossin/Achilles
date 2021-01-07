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
    ce1.condition_concept_id as stratum_1,
    YEAR(condition_era_start_date) as stratum_2,
    p1.gender_concept_id as stratum_3,
    floor((year(condition_era_start_date) - p1.year_of_birth)/10) as stratum_4,
    COUNT_BIG(distinct p1.PERSON_ID) as count_value
  from @cdmDatabaseSchema.person p1
  join @cdmDatabaseSchema.condition_era ce1 on p1.person_id = ce1.person_id
  group by ce1.condition_concept_id,
    YEAR(condition_era_start_date),
    p1.gender_concept_id,
    floor((year(condition_era_start_date) - p1.year_of_birth)/10)
)
SELECT
  @analysisId as analysis_id,
  CAST(stratum_1 AS VARCHAR(255)) as stratum_1,
  cast(stratum_2 as varchar(255)) as stratum_2,
  cast(stratum_3 as varchar(255)) as stratum_3,
  cast(stratum_4 as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
FROM rawData;
