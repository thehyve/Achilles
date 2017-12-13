# @file Achilles
#
# This file is part of Achilles
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author The Hyve
# @author Maxim Moinat

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
listAllMappings <- function(
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
  
  # Execute all the statements
  executeSql(connection, sql)
}

dropAndCreateMappingOverviewTable <- function(connection, schema, dbms) {
  sql <- SqlRender::loadRenderTranslateSql(     
    sqlFilename = "vocabulary_mapping/DDL_SingleMappingsOverview.sql",
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
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename ="vocabulary_mapping/ListMappings.sql",
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
    listAllMappings(connectionDetails, resultsDatabaseSchema)
  }

  # Select everything if no mappingName is given
  if (mappingName == "" || is.null(mappingName)) {
    mappingName = "%" 
  }
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename ="vocabulary_mapping/MappingStats.sql",
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = resultsDatabaseSchema,
    mapping_name = mappingName
  )

  df <- querySql(connection, sql)

  df$PERCENTAGE_OF_ROWS <- df$N_ROWS / sum(df$N_ROWS) * 100
  return(df)
}

#' @title mappingStatsOverview
#' 
#' @description
#' \code{mappingStats} returns an overview of the mappings per mapping type
#'
#' @details
#' \code{mappingStats} returns an overview of the mappings per mapping type
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param withTotalRow   boolean
#' 
#' @return A dataframe with overview of mapping statistics
#' @export
mappingStatsOverview <- function(connectionDetails, resultsDatabaseSchema = "webapi", withTotalRow = FALSE) {
  connection <- connect(connectionDetails)
  
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms)) {
    listAllMappings(connectionDetails, resultsDatabaseSchema)
  }

  sql <- SqlRender::loadRenderTranslateSql(     
    sqlFilename ="vocabulary_mapping/MappingStatsOverview.sql",
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = resultsDatabaseSchema
  )

  df <- querySql(connection, sql)
  
  # Set first column as rownames
  rownames(df) <- df$MAPPING_NAME
  df$MAPPING_NAME <- NULL

  if (withTotalRow) {
    totalRow <- colSums(df[,1:(ncol(df)-1)])
    totalRow <- c(totalRow, 1 - totalRow[["N_ROWS_NOT_MAPPED"]]/totalRow[["N_ROWS"]])
    df["TOTAL" ,] <- totalRow
  }
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
  df <- topMapped_("vocabulary_mapping/TopMappedConcepts.sql", connectionDetails, resultsDatabaseSchema, mappingName, topX)
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
  df <- topMapped_("vocabulary_mapping/TopNotMappedConcepts.sql", connectionDetails, resultsDatabaseSchema, mappingName, topX)
  df$TO_GAIN <- cumsum(df$PERCENTAGE_OF_ROWS)
    
  return(df)
}

#' Convenience function used by both topMapped and topNotMapped
topMapped_ <- function(sqlFilename, connectionDetails, resultsDatabaseSchema, mappingName, topX){
  connection <- connect(connectionDetails)
  # If no mapping overview created yet, do that here
  if (!hasMappingOverview(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)) {
    listAllMappings(connectionDetails, resultsDatabaseSchema)
  }
  
  if (is.null(topX)) {
    topX = "NULL"
  }
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = sqlFilename,
    packageName = "Achilles",
    dbms = connectionDetails$dbms,
    results_database_schema = resultsDatabaseSchema,
    mapping_name = mappingName,
    limit = topX
  )
  
  df <- querySql(connection, sql)
  
  # Coverage as percentage of all records in this mapping type
  total <- getTotalFrequency(connection, resultsDatabaseSchema, connectionDetails$dbms, mappingName)
  df$PERCENTAGE_OF_ROWS <- df$N_ROWS / total * 100
  
  return(df)
}

#' @return numeric The total number of records for the given mappingName
getTotalFrequency <- function(connection, resultsDatabaseSchema = "webapi", dbms, mappingName) {
    if (mappingName == "" || is.null(mappingName)) {
      mappingName = "%" 
    }
  
    query <- SqlRender::translateSql(
      sprintf("SELECT sum(n_rows) AS TOTAL 
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
    } else {
      if (!mappingName %in% getMappingFields()$mapping_id) {
        stop(sprintf("Mapping name '%s' is not recognised, please see 'inst/csv/%s' for a list of all possible mapping ids.", mappingName, "mappingFields.csv"))
      }
    }
    
    mappingName = "%" # hack to report on existence of table
    
    query <- SqlRender::translateSql(
      sprintf("SELECT count(*) AS COUNT 
              FROM %s.achilles_vocab_concept_mappings 
              WHERE mapping_name LIKE '%s'",resultsDatabaseSchema, mappingName),
      targetDialect=dbms
    )
    
    result <- querySql(connection, query$sql)$COUNT
    
    return(result > 0)
}

#' @title Full export of all vocabulary stats
#' 
#' @description
#' \code{exportVocabStats} is a convenience function that creates a csv file for every mapping stat.
#'
#' @details
#' \code{exportVocabStats} is a convenience function that creates a csv file for every mapping stat.
#' The result is one mapping overview csv and a folder for each CDM source to concept mapping type.
#' Each folder contains a detailed mapping statistic and the \code{topX} mapped and not mapped concepts.
#' 
#' @param connectionDetails An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is 'webapi'
#' @param outputPath string in this folder the mapping stats are written.
#' @param mappingName Name of the mapping, e.g. 'condition', 'observation', 'observation_value', ... For a full list see \code{mappingFields.csv}.
#' @param topX
#' 
#' @export
exportVocabStats <- function(connectionDetails, resultsDatabaseSchema = "webapi",  outputPath="./out", topX = 20) {
  mappingNames <- getMappingFields()$mapping_id
  
  df.overview <- mappingStatsOverview(connectionDetails, resultsDatabaseSchema, TRUE)
  dir.create(outputPath, showWarnings = FALSE)
  write.csv(df.overview, file.path(outputPath, "mapping_overview.csv"))
  
  for(mappingName in mappingNames) {
    subPath <- file.path(outputPath, mappingName)

    df.detail <- mappingStats(connectionDetails, resultsDatabaseSchema, mappingName)
    if (nrow(df.detail) > 0) {
      dir.create(subPath, showWarnings = FALSE)
      write.csv(df.detail, file.path(subPath, "mapping_stats.csv"))
    } else {
      # If no mapping stats for this mapping, then there are no concepts at all
      next
    }
    
    df.mapped <- topMapped(connectionDetails, resultsDatabaseSchema, mappingName, topX)
    if (nrow(df.mapped) > 0) {
      write.csv(df.mapped, file.path(subPath, "topMapped.csv"))
    }
    
    df.notmapped <- topNotMapped(connectionDetails, resultsDatabaseSchema, mappingName, topX)
    if (nrow(df.notmapped) > 0) {
      write.csv(df.notmapped, file.path(subPath, "topUnmapped.csv"))
    }
  }
}
