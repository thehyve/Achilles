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
	concept.concept_name AS target_concept_name,
	concept.concept_class_id AS target_concept_class_id,
	frequency		
FROM frequencies
LEFT JOIN @cdm_schema.source_to_concept_map 
	ON frequencies.source_code = source_to_concept_map.source_code
	AND source_to_concept_map.source_vocabulary_id = '@source_vocabulary_id'
JOIN @cdm_schema.concept
  ON concept.concept_id = frequencies.target_concept_id
WHERE frequencies.target_concept_id > 0
ORDER BY frequency DESC
LIMIT @limit