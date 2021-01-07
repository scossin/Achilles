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

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData
as
(
  select num_observations as count_value
  from
	(
  	select 
  	  o1.person_id, 
  	  COUNT_BIG(distinct o1.observation_concept_id) as num_observations
  	from @cdmDatabaseSchema.observation o1 
  	join @cdmDatabaseSchema.observation_period op on o1.person_id = op.person_id
    where o1.observation_date <= op.observation_period_end_date 
      and o1.observation_date >= op.observation_period_start_date
    group by o1.person_id
	) t0
),
overallStats 
as
(
  select 
    CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
  from rawData
),
statsView 
as
(
  select 
    count_value, 
  	count_big(*) as total, 
		row_number() over (order by count_value) as rn
  FROM rawData
  group by count_value
),
priorStats 
as
(
  select 
    s.count_value, 
    s.total, 
    sum(p.total) as accumulated
  from statsView s
  join statsView p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 
  @analysisId as analysis_id,
  o.total as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
into #tempResults_@analysisId
from priorStats p
CROSS JOIN overallStats o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(count_value)
select 
  analysis_id, 
  cast(null as varchar(255)) as stratum_1, 
  cast(null as varchar(255)) as stratum_2, 
  cast(null as varchar(255)) as stratum_3, 
  cast(null as varchar(255)) as stratum_4, 
  cast(null as varchar(255)) as stratum_5,
  count_value, 
  min_value,
  max_value, 
  avg_value, 
  stdev_value,
  median_value, 
  p10_value, 
  p25_value, 
  p75_value, 
  p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_@analysisId
from #tempResults_@analysisId
;

truncate table #tempResults_@analysisId;

drop table #tempResults_@analysisId;
