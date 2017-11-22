#' @title Create table of all source to target concept mappings
#'
#' @description
#' \code{createFullMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#'
#' @details
#' \code{createFullMappingOverview} creates and fills a table with concept mappings for all source/target pairs. These pairs can be found in \code{mappingFields.csv}.
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' 
#' @export
createFullMappingOverview <- function(connectionDetails, resultsDatabaseSchema = "webapi") {
  connection <- connect(connectionDetails)
  
  # Set up new table
  dropMappingOverviewTable(connection, resultsDatabaseSchema, connectionDetails$dbms)
  createMappingOverviewTable(connection, resultsDatabaseSchema, connectionDetails$dbms)
  
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
        	target_concept_id int4 NULL,
        	target_concept_name varchar(255) NULL,
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
  sql <- loadRenderTranslateSql2("vocabulary_mapping/AllMappings.sql",
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

#' @title mappingStats
#' 
#' NOTE: createFullMappingOverview has been run first
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

  connection <- connect(connectionDetails)
  df <- querySql(connection, sql)
  
  df$COVERAGE <- df$FREQUENCY / sum(df$FREQUENCY) * 100
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
  if (is.null(topX)) {
    topX = "NULL"
  }
  sql <- loadRenderTranslateSql2("vocabulary_mapping/TopMappedConcepts.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema,
                                 mapping_name = mappingName,
                                 limit = topX
  )

  connection <- connect(connectionDetails)
  return(querySql(connection, sql))
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
  if (is.null(topX)) {
    topX = "NULL"
  }
  sql <- loadRenderTranslateSql2("vocabulary_mapping/TopNotMappedConcepts.sql",
                                 packageName = "Achilles",
                                 dbms = connectionDetails$dbms,
                                 results_database_schema = resultsDatabaseSchema,
                                 mapping_name = mappingName,
                                 limit = topX
  )

  connection <- connect(connectionDetails)
  return(querySql(connection, sql))
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
