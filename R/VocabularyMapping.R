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
    warning(sprintf("Did not recognise table name '%s'", cdmTable))
    return()
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
    warning(sprintf("Did not recognise table name '%s'", cdmTable))
    return()
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
