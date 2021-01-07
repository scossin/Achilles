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

-- create unique person_id,procedure_concept_id pairs for people with at least 2 different (mapped) procedures.

--HINT DISTRIBUTE_ON_KEY(procedure_concept_id)
select distinct 
  p1.person_id,
  p1.procedure_concept_id
into #unique_pairs_@analysisId
from @cdmDatabaseSchema.procedure_occurrence p1
where p1.procedure_concept_id != 0
  and not exists
  (
    select 1 
    from @cdmDatabaseSchema.procedure_occurrence p2
    where p1.person_id = p2.person_id
      and p2.procedure_concept_id != 0
    group by p2.person_id
      having count(distinct p2.procedure_concept_id) = 1)
  )
;

-- Create ordered pairs of concept_ids, then count and rank them (procedure pairs must have at least 1000 distinct people)

 --HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  @analysisId as analysis_id,
  cast(concept_id_1 as varchar(255)) as stratum_1,
  cast(concept_id_2 as varchar(255)) as stratum_2,
  cast(ranking as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
	num_people as count_value		
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
from 
(
  select 
    concept_id_1, 
    concept_id_2, 
    num_people,
    row_number()over(partition by concept_id_1 order by num_people desc) ranking
  from 
  (
    select
      t1.procedure_concept_id concept_id_1,
      t2.procedure_concept_id concept_id_2,
      count(distinct t1.person_id) num_people
    from #unique_pairs_@analysisId t1 cross join #unique_pairs_@analysisId t2
    where t1.person_id = t2.person_id
      and t1.procedure_concept_id != t2.procedure_concept_id
    group by t1.procedure_concept_id,t2.procedure_concept_id
      having count(distinct t1.person_id) >= 1000 
  ) tmp
) tmp
where ranking <= 15;

truncate table #unique_pairs_@analysisId;
drop table #unique_pairs_@analysisId;