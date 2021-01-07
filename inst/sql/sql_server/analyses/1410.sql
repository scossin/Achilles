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

--HINT DISTRIBUTE_ON_KEY(obs_month)
SELECT DISTINCT 
  YEAR(payer_plan_period_start_date)*100 + MONTH(payer_plan_period_start_date) AS obs_month,
  DATEFROMPARTS(YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1) as obs_month_start,
  EOMONTH(payer_plan_period_start_date) as obs_month_end
INTO #temp_dates_@analysisId
FROM @cdmDatabaseSchema.payer_plan_period
;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
select 
  @analysisId as analysis_id, 
	CAST(obs_month AS VARCHAR(255)) as stratum_1,
	cast(null as varchar(255)) as stratum_2, 
	cast(null as varchar(255)) as stratum_3, 
	cast(null as varchar(255)) as stratum_4, 
	cast(null as varchar(255)) as stratum_5,
	COUNT_BIG(distinct p1.PERSON_ID) as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from @cdmDatabaseSchema.person p1
join @cdmDatabaseSchema.payer_plan_period ppp1 on p1.person_id = ppp1.person_id
cross join #temp_dates_@analysisId
where ppp1.payer_plan_period_START_DATE <= obs_month_start
	and ppp1.payer_plan_period_END_DATE >= obs_month_end
group by obs_month
;

TRUNCATE TABLE #temp_dates_@analysisId;
DROP TABLE #temp_dates_@analysisId;
