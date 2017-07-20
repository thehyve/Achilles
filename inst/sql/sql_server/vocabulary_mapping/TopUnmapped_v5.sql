WITH frequencies(
	source_code,
	target_concept_id,
	frequency
) AS(
	SELECT @concept_source_column, @concept_id_column, count(*)
	FROM @cdm_schema.@cdm_table
	GROUP BY @concept_source_column, @concept_id_column
)
SELECT 
	frequencies.source_code,
	source_to_concept_map.source_vocabulary_id, 
	source_to_concept_map.source_code_description,
	frequency		
FROM frequencies
LEFT JOIN @cdm_schema.source_to_concept_map 
	ON frequencies.source_code = source_to_concept_map.source_code
	AND source_to_concept_map.source_vocabulary_id = '@source_vocabulary_id'
WHERE (frequencies.target_concept_id = 0 OR frequencies.target_concept_id IS NULL)
ORDER BY frequency DESC
LIMIT @limit
