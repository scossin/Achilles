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
  select 
    p.person_id, 
    MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
  from @cdmDatabaseSchema.person p
  join @cdmDatabaseSchema.observation_period op on p.person_id = op.person_id
  group by p.person_id, p.year_of_birth
),
overallStats
as
(
  select 
    CAST(avg(1.0 * age_value) AS FLOAT) as avg_value,
    CAST(stdev(age_value) AS FLOAT) as stdev_value,
    min(age_value) as min_value,
    max(age_value) as max_value,
    count_big(*) as total
  FROM rawData
),
ageStats
as
(
  select 
    age_value, 
    count_big(*) as total, 
    row_number() over (order by age_value) as rn
  from rawData
  group by age_value
),
ageStatsPrior 
as
(
  select 
    s.age_value, 
    s.total, 
    sum(p.total) as accumulated
  from ageStats s
  join ageStats p on p.rn <= s.rn
  group by s.age_value, s.total, s.rn
),
tempResults 
as
(
  select 
    @analysisId as analysis_id,
    o.total as count_value,
  	o.min_value,
  	o.max_value,
  	o.avg_value,
  	o.stdev_value,
  	MIN(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
  	MIN(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
  	MIN(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
  	MIN(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
  	MIN(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
  from ageStatsPrior p
  CROSS JOIN overallStats o
  GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
)
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
from tempResults
;
