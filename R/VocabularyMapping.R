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

#'
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

#'
topUnmapped <- function(connectionDetails, cdmTable, sourceVocabularyId, topX = 10){
  sourceValueColumn = getMainSourceValueColumnName(cdmTable)
  conceptIdColumn = getMainConceptIdColumnName(cdmTable)
  
  ## Get top unmapped
  sql <- loadRenderTranslateSql2("vocabulary_mapping/TopUnmapped_v5.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 cdm_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_source_column = sourceValueColumn,
                                 concept_id_column = conceptIdColumn,
                                 source_vocabulary_id = sourceVocabularyId,
                                 limit = topX
  )
  connection <- connect(connectionDetails)
  result_df <- querySql(connection, sql)
  
  return(result_df)
}

#'
topMapped <- function(connectionDetails, cdmTable, sourceVocabularyId, topX = 10){
  sourceValueColumn = getMainSourceValueColumnName(cdmTable)
  conceptIdColumn = getMainConceptIdColumnName(cdmTable)
  
  ## Get top unmapped
  sql <- loadRenderTranslateSql2("vocabulary_mapping/TopMapped_v5.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 cdm_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_source_column = sourceValueColumn,
                                 concept_id_column = conceptIdColumn,
                                 source_vocabulary_id = sourceVocabularyId,
                                 limit = topX
  )
  connection <- connect(connectionDetails)
  result_df <- querySql(connection, sql)
  
  return(result_df)
}
