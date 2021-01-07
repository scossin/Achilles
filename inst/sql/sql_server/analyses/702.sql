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
    de1.drug_concept_id as stratum_1,
    YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date) as stratum_2,
    COUNT_BIG(distinct PERSON_ID) as count_value
  from @cdmDatabaseSchema.drug_exposure de1
  join @cdmDatabaseSchema.observation_period op on de1.person_id = op.person_id
  where de1.drug_exposure_start_date <= op.observation_period_end_date
    and de1.drug_exposure_end_date >= op.observation_period_start_date
  group by de1.drug_concept_id,
    YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date)
)
SELECT
  @analysisId as analysis_id,
  CAST(stratum_1 AS VARCHAR(255)) as stratum_1,
  cast(stratum_2 as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
FROM rawData;
