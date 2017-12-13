-- Does NOT count null or 0 target_concept_ids
-- Does count null source_codes
SELECT
  mapping_name,
  count(DISTINCT COALESCE(source_code,''))               AS n_source_codes,
  count(DISTINCT NULLIF(target_concept_id, 0))           AS n_target_concepts,
  sum(CAST(NOT is_mapped AS INTEGER))                    AS n_source_codes_not_mapped,
  sum(n_persons)                                         AS n_persons,
  sum(n_persons * CAST(NOT is_mapped AS INTEGER))        AS n_persons_without_mapping,
  sum(n_rows)                                            AS n_rows,
  sum(n_rows * CAST(NOT is_mapped AS INTEGER))           AS n_rows_not_mapped,
  sum(n_rows * CAST(is_mapped AS INTEGER)) / sum(n_rows) AS coverage
FROM @results_database_schema.achilles_vocab_concept_mappings
GROUP BY mapping_name
HAVING sum(n_rows) > 0
ORDER BY coverage DESC, n_source_codes DESC
;
