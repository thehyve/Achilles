WITH frequencies(
	code,
	frequency
) AS(
	SELECT @concept_source_column, count(*)
	FROM @cdm_schema.@cdm_table
	GROUP BY @concept_source_column
),
concept_mapping(
	code,
	source_vocabulary_id,
	target_concept_class_id,
	frequency
) AS(
	SELECT 
		COALESCE(frequencies.code, source_to_concept_map.source_code),
		source_vocabulary_id, 
		CASE WHEN target_concept_id > 0
			 THEN concept.concept_class_id
			 ELSE 'Not Mapped'
		END,
		frequency		
	FROM @cdm_schema.source_to_concept_map
	FULL OUTER JOIN frequencies 
		ON frequencies.code = source_to_concept_map.source_code
		AND source_to_concept_map.source_vocabulary_id = '@source_vocabulary_id'
	LEFT JOIN @cdm_schema.concept
		ON target_concept_id = concept_id
	WHERE source_vocabulary_id = '@source_vocabulary_id' OR source_vocabulary_id IS NULL
),
mapping_stats(
	source_vocabulary_id,
	target_concept_class_id,
	occurrences,
	frequency
) AS( 
	SELECT 
		source_vocabulary_id, 
		target_concept_class_id, 
		count(DISTINCT code),
		sum(frequency)
	FROM concept_mapping
	GROUP BY source_vocabulary_id, target_concept_class_id
),
totals(
	total_occurrences,
	total_frequency
) AS(
	SELECT 
		sum(occurrences) AS total_occurrences, 
		sum(frequency) AS total_frequency 
	FROM mapping_stats
)
-- Occurrence counts any occurrences in cdm_table or source_to_concept_map
SELECT 
  source_vocabulary_id, 
  target_concept_class_id, 
  occurrences, 
  occurrences/total_occurrences * 100 as occurence_percentage,
  frequency, 
  frequency/total_frequency * 100 as coverage
FROM mapping_stats
CROSS JOIN totals
<<<<<<< HEAD
ORDER BY source_vocabulary_id, occurrences DESC
=======
ORDER BY source_vocabulary_id, frequency DESC NULLS LAST
>>>>>>> e00df66... sorting nulls last for mapping frequency

