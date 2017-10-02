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
LEFT JOIN @cdm_schema.concept
  ON concept.concept_id = frequencies.target_concept_id
-- Get either mapped (with a target_concept_id) or unmapped (without target_concept_id)
WHERE (@get_mapped AND frequencies.target_concept_id > 0) 
      OR 
      (NOT @get_mapped AND (frequencies.target_concept_id = 0 OR frequencies.target_concept_id IS NULL))
ORDER BY frequency DESC
LIMIT @limit