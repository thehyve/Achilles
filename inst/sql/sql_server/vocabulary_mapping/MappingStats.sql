SELECT
	mapping_name,
	source_vocabulary_id, 
	target_concept_class_id,
	is_mapped,
	count(DISTINCT source_code) 	  AS n_source_codes, 
  count(DISTINCT target_concept_id) AS n_target_concepts,
	sum(person_count)         AS person_count,
	sum(frequency) 					  AS frequency
FROM @results_database_schema.achilles_vocab_concept_mappings
WHERE mapping_name LIKE '@mapping_name'
GROUP BY source_vocabulary_id, target_concept_class_id, mapping_name, is_mapped
ORDER BY is_mapped DESC, mapping_name, source_vocabulary_id, frequency DESC