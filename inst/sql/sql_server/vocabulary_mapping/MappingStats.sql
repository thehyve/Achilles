SELECT
	mapping_name,
	source_vocabulary_id, 
	target_concept_class_id,
	is_mapped,
	target_standard_concept,
	count(DISTINCT source_code) 	    AS n_source_codes, 
  count(DISTINCT target_concept_id) AS n_target_concepts,
	sum(n_rows) 		  			          AS n_rows
FROM @results_database_schema.achilles_vocab_concept_mappings
WHERE mapping_name LIKE '@mapping_name'
GROUP BY source_vocabulary_id, target_concept_class_id, mapping_name, is_mapped, target_standard_concept
ORDER BY is_mapped DESC, target_standard_concept, mapping_name, source_vocabulary_id, n_rows DESC