#' @title Create table of all source to target concept mappings
#'
#' @description
#' \code{createFullMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#'
#' @details
#' \code{createFullMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' 
#' @export
createFullMappingOverview <- function(connectionDetails, resultsDatabaseSchema = "webapi") {
  connection <- connect(connectionDetails)
  
  # Set up new table
  dropMappingOverviewTable(connection, resultsDatabaseSchema)
  createMappingOverviewTable(connection, resultsDatabaseSchema)
  
  # For every source/target field, create query and concatenate
  mappingTargets <- getMappingFields()
  sql <- ""
  for (i in 1:nrow(mappingTargets)) {
    mappingTarget <- mappingTargets[i,]
    insertQuery <- createMappingOverviewInsertQuery(
      connectionDetails, 
      resultsDatabaseSchema,
      mappingTarget$table_name, 
      mappingTarget$concept_id_field, 
      mappingTarget$source_concept_id, 
      mappingTarget$source_value_field, 
      "", 
      mappingTarget$mapping_id
    )
    sql <- paste(sql,";", insertQuery)
  }
  
  # Execute all the statements in one go
  connection <- connect(connectionDetails)
  executeSql(connection, sql)
}

dropMappingOverviewTable <- function(connection, schema, dbms) {
  dropQuery <- SqlRender::translateSql(
    sprintf("DROP TABLE IF EXISTS %s.achilles_vocab_concept_mappings;", schema),
    targetDialect=dbms
  )
  executeSql(connection, dropQuery$sql)
}

createMappingOverviewTable <- function(connection, schema, dbms) {
  createQuery <- SqlRender::translateSql(
      sprintf("CREATE TABLE %s.achilles_vocab_concept_mappings (
        	mapping_name varchar(50) NULL,
        	source_code varchar(50) NULL,
        	source_vocabulary_id varchar(20) NULL,
        	source_code_description varchar(255) NULL,
        	concept_id int4 NULL,
        	concept_name varchar(255) NULL,
        	target_concept_class_id varchar NULL,
          is_mapped boolean NOT NULL,
        	frequency int8 NULL
      );",schema),
      targetDialect=dbms
  )
  executeSql(connection, createQuery$sql)
}

getMappingFields <- function() {
  pathToCsv <- system.file("csv", "mappingFields.csv", package = "Achilles")
  return(utils::read.csv(pathToCsv))
}

createMappingOverviewInsertQuery <- function(connectionDetails, resultsDatabaseSchema, cdmTable, conceptIdColumn, sourceIdColumn, sourceValueColumn, sourceVocabularyIds, mappingName = ""){
  sourceVocabularyIds <- paste("'", sourceVocabularyIds, "'", collapse = ",", sep="")
  
  if (sourceIdColumn == "") {
    sourceIdColumn = "CAST(NULL AS INTEGER)"
  }
  
  if (sourceValueColumn == "") {
    sourceValueColumn = "CAST(NULL AS VARCHAR)"
  }
  
  # TODO: replace by regular loadRenderTranslateSql
  sql <- loadRenderTranslateSql2("vocabulary_mapping/MappingOverview.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema,
                                 cdm_database_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_id_column = conceptIdColumn,
                                 source_concept_id_column = sourceIdColumn,
                                 source_value_column = sourceValueColumn,
                                 source_vocabulary_id_list = sourceVocabularyIds,
                                 mapping_name = mappingName
  )
  
  return(sql)
}

#' @title vocabularyMapping
#'
#' @description
#' \code{vocabularyMapping} creates mapping statistics for a specific source vocabulary
#'
#' @details
#' \code{vocabularyMapping} creates mapping statistics for a specific source vocabulary
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmTable Name of cdm domain table to inspect, one of `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement` or `observation`.
#' @param sourceVocabularyId  Name of the source vocabulary to evaluate
#' 
#' @return A dataframe with results
#' 
#' @export
vocabularyMapping <- function(connectionDetails, cdmTable, sourceVocabularyId){
  sourceValueColumn = getMainSourceValueColumnName(cdmTable)

  sql <- loadRenderTranslateSql2("vocabulary_mapping/AchillesMapping_v5.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 cdm_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_source_column = sourceValueColumn,
                                 source_vocabulary_id = sourceVocabularyId
  )

  connection <- connect(connectionDetails)
  result_df <- querySql(connection, sql)

  return(result_df)
}

#' @title Top Concepts Mapped
#'
#' @description
#' \code{topMapped} returns a dataframe of the \code{topX} mapped concepts, based on occurrence
#'
#' @details
#' \code{topMapped} returns a dataframe of the \code{topX} mapped concepts, based on occurrence
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmTable Name of cdm domain table to inspect, one of `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement` or `observation`.
#' @param sourceVocabularyId  Name of the source vocabulary to evaluate
#' @param topX
#' 
#' @return A dataframe with results
#' 
#' @export
topMapped <- function(connectionDetails, cdmTable, sourceVocabularyId, topX = 10){
  return(topSourceToTargetConcepts_(connectionDetails, cdmTable, sourceVocabularyId, topX, TRUE))
}

#' @title Top Concepts Not Mapped
#'
#' @description
#' \code{topMapped} returns a dataframe of the \code{topX} unmapped concepts, based on occurrence
#'
#' @details
#' \code{topMapped} returns a dataframe of the \code{topX} unmapped concepts, based on occurrence
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmTable Name of cdm domain table to inspect, one of `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement` or `observation`.
#' @param sourceVocabularyId  Name of the source vocabulary to evaluate
#' @param topX
#' 
#' @return A dataframe with the results
#' 
#' @export
topUnmapped <- function(connectionDetails, cdmTable, sourceVocabularyId, topX = 10){
  return(topSourceToTargetConcepts_(connectionDetails, cdmTable, sourceVocabularyId, topX, FALSE))
}

### Supportive functions ###

topSourceToTargetConcepts_ <- function(connectionDetails, cdmTable, sourceVocabularyId, topX, getMapped) {
  sourceValueColumn = getMainSourceValueColumnName(cdmTable)
  conceptIdColumn = getMainConceptIdColumnName(cdmTable)
  
  ## Get top x mapped or unmapped concepts
  sql <- loadRenderTranslateSql2("vocabulary_mapping/TopConcepts_v5.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 cdm_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_source_column = sourceValueColumn,
                                 concept_id_column = conceptIdColumn,
                                 source_vocabulary_id = sourceVocabularyId,
                                 get_mapped = getMapped,
                                 limit = topX
  )
  connection <- connect(connectionDetails)
  result_df <- querySql(connection, sql)
  
  return(result_df)
}

getMainSourceValueColumnName <- function(cdmTable) {
  if (cdmTable == 'condition_occurrence') {
    return('condition_source_value')
    
  } else if (cdmTable == 'procedure_occurrence') {
    return('procedure_source_value')
    
  } else if (cdmTable == 'drug_exposure') {
    return('drug_source_value')
    
  } else if (cdmTable == 'measurement') {
    return('measurement_source_value')
    
  } else if (cdmTable == 'observation') {
    return('observation_source_value')
    
  } else {
    stop(sprintf("Did not recognise table name '%s'", cdmTable))
  }
}

getMainConceptIdColumnName <- function(cdmTable) {
  if (cdmTable == 'condition_occurrence') {
    return('condition_concept_id')
    
  } else if (cdmTable == 'procedure_occurrence') {
    return('procedure_concept_id')
    
  } else if (cdmTable == 'drug_exposure') {
    return('drug_concept_id')
    
  } else if (cdmTable == 'measurement') {
    return('measurement_concept_id')
    
  } else if (cdmTable == 'observation') {
    return('observation_concept_id')
    
  } else {
    stop(sprintf("Did not recognise table name '%s'", cdmTable))
  }
}

loadRenderTranslateSql2 <- function (sqlFilename, packageName, dbms = "sql server", ..., 
                                     oracleTempSchema = NULL) 
{
  pathToSql <- system.file(paste("sql/", gsub(" ", "_", dbms), 
                                 sep = ""), sqlFilename, package = packageName)
  mustTranslate <- !file.exists(pathToSql)
  if (mustTranslate) {
    pathToSql <- system.file(paste("sql/", "sql_server", 
                                   sep = ""), sqlFilename, package = packageName)
  }
  parameterizedSql <- readChar(pathToSql, file.info(pathToSql)$size)
  renderedSql <- renderSql(parameterizedSql[1], ...)$sql
  if (mustTranslate) 
    renderedSql <- translateSql(sql = renderedSql, targetDialect = dbms, 
                                oracleTempSchema = oracleTempSchema)$sql
  renderedSql
}
