SELECT 
  mapping_name,
	source_code,
	source_vocabulary_id,
	source_code_description,
	target_concept_id,
	target_concept_name,
	target_concept_class_id,
	target_standard_concept,
	n_persons,
	n_rows
FROM @results_database_schema.achilles_vocab_concept_mappings
WHERE mapping_name LIKE '@mapping_name' AND is_mapped
ORDER BY n_rows DESC
LIMIT @limit
;