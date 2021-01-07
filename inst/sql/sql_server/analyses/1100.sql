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
    left(l1.zip,3) as stratum_1,
    COUNT_BIG(distinct person_id) as count_value
  from @cdmDatabaseSchema.person p1
  join @cdmDatabaseSchema.location l1 on p1.location_id = l1.location_id
  where p1.location_id is not null
    and l1.zip is not null
  group by left(l1.zip,3)
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
