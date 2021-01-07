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


select 
  @analysisId as analysis_id,  
	cast(null as varchar(255)) as stratum_1, 
	cast(null as varchar(255)) as stratum_2, 
	cast(null as varchar(255)) as stratum_3, 
	cast(null as varchar(255)) as stratum_4, 
	cast(null as varchar(255)) as stratum_5,
  CAST(a.cnt AS BIGINT) AS count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from
(
  select 
    COUNT_BIG(*) cnt 
  from 
  (
    select distinct 
      person_id 
    from @cdmDatabaseSchema.condition_occurrence
    intersect
    select distinct 
      person_id 
    from @cdmDatabaseSchema.drug_exposure
  ) b
) a
;
