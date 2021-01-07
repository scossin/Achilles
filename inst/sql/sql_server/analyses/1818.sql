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


--HINT DISTRIBUTE_ON_KEY(person_id)
select 
  person_id,
  measurement_concept_id,
  unit_concept_id,
  CAST(case when value_as_number < range_low then 'Below Range Low'
  		when value_as_number >= range_low and value_as_number <= range_high then 'Within Range'
  		when value_as_number > range_high then 'Above Range High'
  		else 'Other' end AS VARCHAR(255)) as stratum_3
into #rawData_@analysisId
from @cdmDatabaseSchema.measurement
where value_as_number is not null
  and unit_concept_id is not null
  and range_low is not null
  and range_high is not null;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
select 
  @analysisId as analysis_id,  
	CAST(measurement_concept_id AS VARCHAR(255)) as stratum_1,
	CAST(unit_concept_id AS VARCHAR(255)) as stratum_2,
	CAST(stratum_3 AS VARCHAR(255)) as stratum_3,
	cast(null as varchar(255)) as stratum_4, 
	cast(null as varchar(255)) as stratum_5,
	COUNT_BIG(PERSON_ID) as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from #rawData_@analysisId
group by measurement_concept_id,
	unit_concept_id,
  stratum_3
;

truncate table #rawData_@analysisId;
drop table #rawData_@analysisId;

