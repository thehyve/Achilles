WITH concepts(
	target_concept_id,
	source_concept_id,
	source_value,
	person_id
) AS (
	SELECT 
		@concept_id_column,
		@source_concept_id_column,
		@source_value_column,
		@person_id_column
	FROM @cdm_database_schema.@cdm_table
),
all_mappings AS (
	SELECT 
		-- Use _source_concept_id as source if given, otherwise use source_value with given source_vocabulary_id
		COALESCE(source_concept.concept_code, stcm.source_code, source_value) AS source_code,
		COALESCE(source_concept.vocabulary_id, stcm.source_vocabulary_id) AS source_vocabulary_id,
		COALESCE(source_concept.concept_name, stcm.source_code_description) AS source_code_description,
		target_concept.concept_id AS target_concept_id,
		target_concept.concept_name AS target_concept_name,
		target_concept.concept_class_id AS target_concept_class_id,
		CASE
			WHEN target_concept.concept_id > 0 	THEN TRUE
			ELSE FALSE
		END AS is_mapped,
		person_id
	FROM concepts
	LEFT JOIN @cdm_database_schema.concept AS target_concept 
	  ON target_concept_id = target_concept.concept_id
	LEFT JOIN @cdm_database_schema.concept AS source_concept 
	  ON source_concept_id = source_concept.concept_id
	LEFT JOIN @cdm_database_schema.source_to_concept_map AS stcm 
		ON source_value = stcm.source_code
		AND stcm.source_vocabulary_id IN (@source_vocabulary_id_list)
	WHERE concepts.target_concept_id IS NOT NULL -- filters empty _concept_id (if concept not required)
)
INSERT INTO @results_database_schema.achilles_vocab_concept_mappings (
  mapping_name,
	source_code,
	source_vocabulary_id,
	source_code_description,
	target_concept_id,
	target_concept_name,
	target_concept_class_id,
	is_mapped,
	person_count,
	frequency
)
SELECT
  '@mapping_name',
  source_code, 
  source_vocabulary_id, 
  source_code_description, 
  target_concept_id, 
  target_concept_name, 
  target_concept_class_id, 
  is_mapped,
  count(DISTINCT person_id),
  count(*)
FROM all_mappings 
GROUP BY source_code, source_vocabulary_id, source_code_description, target_concept_id, target_concept_name, target_concept_class_id, is_mapped
