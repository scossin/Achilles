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


select distinct 
  YEAR(payer_plan_period_start_date) as obs_year 
INTO #temp_dates_@analysisId
from @cdmDatabaseSchema.payer_plan_period
;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
select 
  @analysisId as analysis_id,  
	CAST(t1.obs_year AS VARCHAR(255)) as stratum_1, 
	cast(null as varchar(255)) as stratum_2, 
	cast(null as varchar(255)) as stratum_3, 
	cast(null as varchar(255)) as stratum_4, 
	cast(null as varchar(255)) as stratum_5,
	COUNT_BIG(distinct p1.PERSON_ID) as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from @cdmDatabaseSchema.person p1
join @cdmDatabaseSchema.payer_plan_period ppp1 on p1.person_id = ppp1.person_id
cross join #temp_dates_@analysisId t1 
where year(ppp1.payer_plan_period_START_DATE) <= t1.obs_year
	and year(ppp1.payer_plan_period_END_DATE) >= t1.obs_year
group by t1.obs_year
;

truncate table #temp_dates_@analysisId;
drop table #temp_dates_@analysisId;
