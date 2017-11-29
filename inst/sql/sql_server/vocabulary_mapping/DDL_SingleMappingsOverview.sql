DROP TABLE IF EXISTS @results_database_schema.achilles_vocab_concept_mappings;
CREATE TABLE @results_database_schema.achilles_vocab_concept_mappings (
  	mapping_name varchar(50) NULL,
  	source_code varchar(50) NULL,
  	source_vocabulary_id varchar(20) NULL,
  	source_code_description varchar(255) NULL,
  	target_concept_id int4 NULL,
  	target_concept_name varchar(255) NULL,
  	target_concept_class_id varchar NULL,
  	target_standard_concept char NULL,
    is_mapped boolean NOT NULL,
  	n_persons int8 NULL,
  	n_rows int8 NULL
);