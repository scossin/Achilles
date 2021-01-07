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
  cast(payer_source_concept_id AS varchar(255)) AS stratum_1,
  cast(null AS varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_big(*) AS count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId 
from @cdmDatabaseSchema.payer_plan_period
group by payer_source_concept_id;

