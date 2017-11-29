#' @title Create table of all source to target concept mappings
#'
#' @description
#' \code{createMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#' The \code{_source_vocabularies} are used to retrieve the \code{source_code_description} of \code{_source_value}'s. This gives more informative overviews of the top (not) mapped concepts.
#'
#' @details
#' \code{createMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param condition_source_vocabularies		string or vector ids of the source vocabularies used to map condition_source_values to condition_concept_ids via the source_to_concept_map table.
#' 
#' @export
createMappingOverview <- function(
    connectionDetails, 
    resultsDatabaseSchema = "webapi",
    condition_source_vocabularies = "",
    drug_source_vocabularies = "",
    device_source_vocabularies = drug_source_vocabularies,
    procedure_source_vocabularies = "",
    specimen_source_vocabularies = "",
    measurement_source_vocabularies = "",
    measurement_unit_source_vocabularies = "",
    measurement_value_source_vocabularies = "",
    observation_source_vocabularies = "",
    observation_unit_source_vocabularies = measurement_unit_source_vocabularies,
    observation_qualifier_source_vocabularies = "",
    death_source_vocabularies = "",
    specialty_source_vocabularies = ""
  ) {
  connection <- connect(connectionDetails)
  
  # Set up new table
  dropAndCreateMappingOverviewTable(connection, resultsDatabaseSchema, connectionDetails$dbms)
  # createMappingOverviewTable(connection, resultsDatabaseSchema, connectionDetails$dbms)
  
  # For every source/target field, create query and concatenate
  mappingTargets <- getMappingFields()
  sql <- ""
  for (i in 1:nrow(mappingTargets)) {
    mappingTarget <- mappingTargets[i,]
    
    vocab_ids <- switch(
         EXPR = as.character(mappingTarget$mapping_id)
        ,condition             = condition_source_vocabularies
        ,drug                  = drug_source_vocabularies
        ,procedure             = procedure_source_vocabularies
        ,device                = device_source_vocabularies
        ,specimen              = specimen_source_vocabularies
        ,measurement           = measurement_source_vocabularies
        ,measurement_unit      = measurement_unit_source_vocabularies
        ,measurement_value     = measurement_value_source_vocabularies
        ,observation           = observation_source_vocabularies
        ,observation_unit      = observation_unit_source_vocabularies
        ,observation_qualifier = observation_qualifier_source_vocabularies
        ,death                 = death_source_vocabularies
        ,provider_specialty    = specialty_source_vocabularies
    )

    insertQuery <- createMappingOverviewInsertQuery(
      connectionDetails, 
      resultsDatabaseSchema,
      mappingTarget$table_name, 
      mappingTarget$concept_id_field, 
      mappingTarget$source_concept_id, 
      mappingTarget$source_value_field, 
      vocab_ids, 
      mappingTarget$mapping_id
    )
    sql <- paste(sql,";", insertQuery)
  }
  
  # Execute all the statements in one go
  connection <- connect(connectionDetails)
  executeSql(connection, sql)
}

dropAndCreateMappingOverviewTable <- function(connection, schema, dbms) {
  sql <- loadRenderTranslateSql2(
    "vocabulary_mapping/DDL_SingleMappingsOverview.sql",
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = schema
  )
  executeSql(connection, sql)
}

getMappingFields <- function() {
  pathToCsv <- system.file("csv", "mappingFields.csv", package = "Achilles")
  return(utils::read.csv(pathToCsv))
}

createMappingOverviewInsertQuery <- function(connectionDetails, resultsDatabaseSchema, cdmTable, conceptIdColumn, sourceIdColumn, sourceValueColumn, sourceVocabularyIds, mappingName = ""){
  sourceVocabularyIds <- paste("'", sourceVocabularyIds, "'", collapse = ",", sep="")
  
  if (sourceIdColumn == "") {
    sourceIdColumn <- "CAST(NULL AS INTEGER)"
  }
  
  if (sourceValueColumn == "") {
    sourceValueColumn <- "CAST(NULL AS VARCHAR)"
  }
  
  personColumn <- "person_id"
  if (cdmTable == "care_site" || cdmTable == "provider") {
    personColumn <- "CAST(NULL AS INTEGER)"
  }
  
  # TODO: replace by regular loadRenderTranslateSql
  sql <- loadRenderTranslateSql2("vocabulary_mapping/SingleMappingsOverview.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema,
                                 cdm_database_schema = connectionDetails$schema,
                                 cdm_table = cdmTable,
                                 concept_id_column = conceptIdColumn,
                                 source_concept_id_column = sourceIdColumn,
                                 source_value_column = sourceValueColumn,
                                 source_vocabulary_id_list = sourceVocabularyIds,
                                 mapping_name = mappingName,
                                 person_id_column = personColumn
  )
  
  return(sql)
}

#' @title mappingStats
#' 
#' @description
#' \code{mappingStats} returns mapping statistics for all or for a specific mapping
#'
#' @details
#' \code{mappingStats} returns mapping statistics for all or for a specific mapping
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param mappingName Name of the mapping, e.g. 'condition', 'observation', 'observation_value', ... For a full list see \code{mappingFields.csv}. Default to all mappings (NULL)
#'
#' @return A dataframe with results
#'
#' @export
mappingStats <- function(connectionDetails, resultsDatabaseSchema = "webapi", mappingName = "") {
  connection <- connect(connectionDetails)
  
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)) {
    createMappingOverview(connectionDetails, resultsDatabaseSchema)
  }

  # Select everything if no mappingName is given
  if (mappingName == "" || is.null(mappingName)) {
    mappingName = "%" 
  }
  
  sql <- loadRenderTranslateSql2("vocabulary_mapping/MappingStats.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema,
                                 mapping_name = mappingName
  )

  df <- querySql(connection, sql)
  
  df$COVERAGE <- df$FREQUENCY / sum(df$FREQUENCY) * 100
  return(df)
}

mappingStatsHighLevel <- function(connectionDetails, resultsDatabaseSchema = "webapi") {
  connection <- connect(connectionDetails)
  
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms)) {
    createMappingOverview(connectionDetails, resultsDatabaseSchema)
  }

  sql <- loadRenderTranslateSql2("vocabulary_mapping/MappingStatsHighLevel.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema
  )

  df <- querySql(connection, sql)
  return(df)
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
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param mappingName Name of the mapping, e.g. 'condition', 'observation', 'observation_value', ... For a full list see \code{mappingFields.csv}.
#' @param topX
#' 
#' @return A dataframe with results
#' 
#' @export
topMapped <- function(connectionDetails, resultsDatabaseSchema = "webapi", mappingName, topX = 20){
  connection <- connect(connectionDetails)
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)) {
    createMappingOverview(connectionDetails, resultsDatabaseSchema)
  }
  
  if (is.null(topX)) {
    topX = "NULL"
  }
  
  sql <- loadRenderTranslateSql2(
    "vocabulary_mapping/TopMappedConcepts.sql",
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = resultsDatabaseSchema,
    mapping_name = mappingName,
    limit = topX
  )

  df <- querySql(connection, sql)
  
  # Coverage as percentage of all records in this mapping type
  total <- getTotalFrequency(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)
  df$COVERAGE <- df$FREQUENCY / total * 100
  
  return(df)
}

#' @title Top Concepts Not Mapped
#'
#' @description
#' \code{topNotMapped} returns a dataframe of the \code{topX} unmapped concepts, based on occurrence
#'
#' @details
#' \code{topNotMapped} returns a dataframe of the \code{topX} unmapped concepts, based on occurrence
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param mappingName Name of the mapping, e.g. 'condition', 'observation', 'observation_value', ... For a full list see \code{mappingFields.csv}.
#' @param topX
#' 
#' @return A dataframe with the results
#' 
#' @export
topNotMapped <- function(connectionDetails, resultsDatabaseSchema = "webapi", mappingName, topX = 20){
  connection <- connect(connectionDetails)
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)) {
    createMappingOverview(connectionDetails, resultsDatabaseSchema)
  }
  
  if (is.null(topX)) {
    topX = "NULL"
  }
  
  sql <- loadRenderTranslateSql2(
    "vocabulary_mapping/TopNotMappedConcepts.sql",
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = resultsDatabaseSchema,
    mapping_name = mappingName,
    limit = topX
  )

  df <- querySql(connection, sql)
  
  # Coverage as percentage of all records in this mapping type
  total <- getTotalFrequency(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)
  df$COVERAGE <- df$FREQUENCY / total * 100
  # To Gain percentage if topX mapped
  df$TO_GAIN <- cumsum(df$COVERAGE)
    
  return(df)
}

#' @return numeric The total number of records for the given mappingName
getTotalFrequency <- function(connection, resultsDatabaseSchema = "webapi", dbms, mappingName) {
    if (mappingName == "" || is.null(mappingName)) {
      mappingName = "%" 
    }
  
    query <- SqlRender::translateSql(
      sprintf("SELECT sum(frequency) AS TOTAL 
              FROM %s.achilles_vocab_concept_mappings 
              WHERE mapping_name LIKE '%s'",resultsDatabaseSchema, mappingName),
      targetDialect=dbms
    )
    return(querySql(connection, query$sql)$TOTAL)
}

#' @return TRUE if \code{achilles_vocab_concept_mappings} contains records for the given mappingName. FALSE otherwise
hasMappingOverview <- function(connection, resultsDatabaseSchema = "webapi", dbms,  mappingName = NULL) {
    if (mappingName == "" || is.null(mappingName)) {
      mappingName = "%" 
    }
  
    query <- SqlRender::translateSql(
      sprintf("SELECT count(*) AS COUNT 
              FROM %s.achilles_vocab_concept_mappings 
              WHERE mapping_name LIKE '%s'",resultsDatabaseSchema, mappingName),
      targetDialect=dbms
    )
    
    result <- querySql(connection, query$sql)$COUNT
    
    return(result > 0)
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
