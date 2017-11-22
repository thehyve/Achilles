SELECT *
FROM @results_database_schema.achilles_vocab_concept_mappings
WHERE mapping_name = '@mapping_name' AND is_mapped
ORDER BY frequency DESC
LIMIT @limit
;