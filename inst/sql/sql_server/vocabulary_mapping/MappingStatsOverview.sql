SELECT
	mapping_name,
	count(DISTINCT source_code) 	    AS n_source_codes, 
  count(DISTINCT target_concept_id) AS n_target_concepts,
	sum(n_persons)             		    AS n_persons,	
	sum(n_rows)                       AS n_rows_total,
	sum(n_rows * is_mapped :: integer)             AS n_rows_mapped,
	sum(n_rows * is_mapped :: integer)/sum(n_rows) AS coverage
FROM @results_database_schema.achilles_vocab_concept_mappings
GROUP BY mapping_name
HAVING sum(n_rows) > 0
ORDER BY coverage DESC, n_source_codes DESC
