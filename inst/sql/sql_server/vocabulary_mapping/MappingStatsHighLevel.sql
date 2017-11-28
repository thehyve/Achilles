SELECT
	mapping_name,
	count(DISTINCT source_code) 	    AS n_source_codes, 
  count(DISTINCT target_concept_id) AS n_target_concepts,
	sum(person_count)         		    AS person_count,	
	sum(frequency) AS n_rows_total,
	sum(frequency * is_mapped :: integer)/sum(frequency) AS coverage
FROM @results_database_schema.achilles_vocab_concept_mappings
GROUP BY mapping_name
HAVING sum(frequency) > 0
ORDER BY coverage DESC, n_source_codes DESC
