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

{cdmVersion == '5'}?{

	--HINT DISTRIBUTE_ON_KEY(stratum_1)
	select 
	  @analysisId as analysis_id, 
		CAST(revenue_code_concept_id AS VARCHAR(255)) as stratum_1,
		cast(null as varchar(255)) as stratum_2, 
		cast(null as varchar(255)) as stratum_3, 
		cast(null as varchar(255)) as stratum_4, 
		cast(null as varchar(255)) as stratum_5,
		COUNT_BIG(pc1.procedure_cost_ID) as count_value
	into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
	from @cdmDatabaseSchema.procedure_cost pc1
	where revenue_code_concept_id is not null
	group by revenue_code_concept_id
	;
	
}:{

	--HINT DISTRIBUTE_ON_KEY(stratum_1)
	select 
	  @analysisId as analysis_id, 
		CAST(revenue_code_concept_id AS VARCHAR(255)) as stratum_1,
		cast(null as varchar(255)) as stratum_2, 
		cast(null as varchar(255)) as stratum_3, 
		cast(null as varchar(255)) as stratum_4, 
		cast(null as varchar(255)) as stratum_5,
		COUNT_BIG(pc1.cost_id) as count_value
	into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_@analysisId
	from @cdmDatabaseSchema.cost pc1
	where revenue_code_concept_id is not null
	  and pc1.cost_domain_id = 'Procedure'
	group by revenue_code_concept_id
	;

}
