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
select 
  @analysisId as analysis_id,  
  CAST(p.year_of_birth AS VARCHAR(255)) as stratum_1,
  CAST(p.gender_concept_id AS VARCHAR(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3, 
  cast(null as varchar(255)) as stratum_4, 
  cast(null as varchar(255)) as stratum_5,
  COUNT_BIG(distinct p.person_id) as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from @cdmDatabaseSchema.person p
where not exists
(
  select 1
  from @cdmDatabaseSchema.death d
  where p.person_id = d.person_id
)
group by YEAR_OF_BIRTH, gender_concept_id;
