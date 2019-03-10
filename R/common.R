# @file Common
#
# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of Achilles
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author Observational Health Data Sciences and Informatics
# @author Martijn Schuemie
# @author Patrick Ryan
# @author Vojtech Huser
# @author Chris Knoll
# @author Ajit Londhe
# @author Taha Abdul-Basser


#' Drop all possible scratch tables
#' 
#' @details 
#' Drop all possible Achilles, Heel, and Concept Hierarchy scratch tables
#' 
#' @param connectionDetails                An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param scratchDatabaseSchema            string name of database schema that Achilles scratch tables were written to. 
#' @param tempAchillesPrefix               The prefix to use for the "temporary" (but actually permanent) Achilles analyses tables. Default is "tmpach"
#' @param tempHeelPrefix                   The prefix to use for the "temporary" (but actually permanent) Heel tables. Default is "tmpheel"
#' @param numThreads                       The number of threads to use to run this function. Default is 1 thread.
#' @param tableTypes                       The types of Achilles scratch tables to drop: achilles or heel or concept_hierarchy or all 3
#' @param outputFolder                     Path to store logs and SQL files
#' @param verboseMode                      Boolean to determine if the console will show all execution steps. Default = TRUE  
#' 
#' @export
dropAllScratchTables <- function(connectionDetails, 
                                 scratchDatabaseSchema, 
                                 tempAchillesPrefix = "tmpach", 
                                 tempHeelPrefix = "tmpheel", 
                                 numThreads = 1,
                                 tableTypes = c("achilles", "heel", "concept_hierarchy"),
                                 outputFolder,
                                 verboseMode = TRUE) {
  
  # Log execution --------------------------------------------------------------------------------------------------------------------
  
  unlink(file.path(outputFolder, "log_dropScratchTables.txt"))
  if (verboseMode) {
    appenders <- list(ParallelLogger::createConsoleAppender(),
                      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutParallel, 
                                                         fileName = file.path(outputFolder, "log_dropScratchTables.txt")))    
  } else {
    appenders <- list(ParallelLogger::createFileAppender(layout = ParallelLogger::layoutParallel, 
                                                         fileName = file.path(outputFolder, "log_dropScratchTables.txt")))
  }
  logger <- ParallelLogger::createLogger(name = "dropAllScratchTables",
                                         threshold = "INFO",
                                         appenders = appenders)
  ParallelLogger::registerLogger(logger) 
  
  
  # Initialize thread and scratchDatabaseSchema settings ----------------------------------------------------------------
  
  schemaDelim <- "."
  
  if (numThreads == 1 || scratchDatabaseSchema == "#") {
    numThreads <- 1
    
    if (.supportsTempTables(connectionDetails)) {
      scratchDatabaseSchema <- "#"
      schemaDelim <- "s_"
    }
  }
  
  if ("achilles" %in% tableTypes) {
    
    # Drop Achilles Scratch Tables ------------------------------------------------------
    
    analysisDetails <- getAnalysisDetails()
    
    resultsTables <- lapply(analysisDetails$ANALYSIS_ID[analysisDetails$DISTRIBUTION <= 0], function(id) {
      sprintf("%s_%d", tempAchillesPrefix, id)
    })
    
    resultsDistTables <- lapply(analysisDetails$ANALYSIS_ID[abs(analysisDetails$DISTRIBUTION) == 1], function(id) {
      sprintf("%s_dist_%d", tempAchillesPrefix, id)
    })
    
    dropSqls <- lapply(c(resultsTables, resultsDistTables), function(scratchTable) {
      sql <- SqlRender::render("IF OBJECT_ID('@scratchDatabaseSchema@schemaDelim@scratchTable', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema@schemaDelim@scratchTable;", 
                               scratchDatabaseSchema = scratchDatabaseSchema,
                               schemaDelim = schemaDelim,
                               scratchTable = scratchTable)
      sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
    })
    
    cluster <- ParallelLogger::makeCluster(numberOfThreads = numThreads, singleThreadToMain = TRUE)
    dummy <- ParallelLogger::clusterApply(cluster = cluster, 
                                          x = dropSqls, 
                                          function(sql) {
                                            connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
                                            tryCatch({
                                              DatabaseConnector::executeSql(connection = connection, sql = sql)  
                                            }, error = function(e) {
                                              ParallelLogger::logError(sprintf("Drop Achilles Scratch Table -- ERROR (%s)", e))  
                                            }, finally = {
                                              DatabaseConnector::disconnect(connection = connection)
                                            })
                                          })
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
  
  if ("heel" %in% tableTypes) {
    # Drop Parallel Heel Scratch Tables ------------------------------------------------------
    
    parallelFiles <- list.files(path = file.path(system.file(package = "Achilles"), 
                                                 "sql/sql_server/heels/parallel"), 
                                recursive = TRUE, 
                                full.names = FALSE, 
                                all.files = FALSE,
                                pattern = "\\.sql$")
    
    parallelHeelTables <- lapply(parallelFiles, function(t) tolower(paste(tempHeelPrefix,
                                                                          trimws(tools::file_path_sans_ext(basename(t))),
                                                                          sep = "_")))
    
    dropSqls <- lapply(parallelHeelTables, function(scratchTable) {
      sql <- SqlRender::render("IF OBJECT_ID('@scratchDatabaseSchema@schemaDelim@scratchTable', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema@schemaDelim@scratchTable;", 
                               scratchDatabaseSchema = scratchDatabaseSchema,
                               schemaDelim = schemaDelim,
                               scratchTable = scratchTable)
      sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
    })
    
    cluster <- ParallelLogger::makeCluster(numberOfThreads = numThreads, singleThreadToMain = TRUE)
    dummy <- ParallelLogger::clusterApply(cluster = cluster, 
                                          x = dropSqls, 
                                          function(sql) {
                                            connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
                                            tryCatch({
                                              DatabaseConnector::executeSql(connection = connection, sql = sql)  
                                            }, error = function(e) {
                                              ParallelLogger::logError(sprintf("Drop Heel Scratch Table -- ERROR (%s)", e))  
                                            }, finally = {
                                              DatabaseConnector::disconnect(connection = connection)
                                            })
                                          })
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
  
  if ("concept_hierarchy" %in% tableTypes) {
    # Drop Concept Hierarchy Tables ------------------------------------------------------
    
    conceptHierarchyTables <- c("condition", "drug", "drug_era", "meas", "obs", "proc")
    
    dropSqls <- lapply(conceptHierarchyTables, function(scratchTable) {
      sql <- SqlRender::render("IF OBJECT_ID('@scratchDatabaseSchema@schemaDelim@tempAchillesPrefix@scratchTable', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix@scratchTable;", 
                               scratchDatabaseSchema = scratchDatabaseSchema,
                               schemaDelim = schemaDelim,
                               tempAchillesPrefix = tempAchillesPrefix,
                               scratchTable = scratchTable)
      sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
    })
    
    cluster <- ParallelLogger::makeCluster(numberOfThreads = numThreads, singleThreadToMain = TRUE)
    dummy <- ParallelLogger::clusterApply(cluster = cluster, 
                                          x = dropSqls, 
                                          function(sql) {
                                            connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
                                            tryCatch({
                                              DatabaseConnector::executeSql(connection = connection, sql = sql)  
                                            }, error = function(e) {
                                              ParallelLogger::logError(sprintf("Drop Concept Hierarchy Scratch Table -- ERROR (%s)", e))  
                                            }, finally = {
                                              DatabaseConnector::disconnect(connection = connection)
                                            })
                                          })
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
  
  ParallelLogger::unregisterLogger("dropAllScratchTables")
}

.getCdmVersion <- function(connectionDetails, 
                           cdmDatabaseSchema) {
  sql <- SqlRender::render(sql = "select cdm_version from @cdmDatabaseSchema.cdm_source",
                           cdmDatabaseSchema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  cdmVersion <- tryCatch({
    c <- tolower((DatabaseConnector::querySql(connection = connection, sql = sql))[1,])
    gsub(pattern = "v", replacement = "", x = c)
  }, error = function (e) {
    ""
  }, finally = {
    DatabaseConnector::disconnect(connection = connection)
    rm(connection)
  })
  
  cdmVersion
}

.supportsTempTables <- function(connectionDetails) {
  !(connectionDetails$dbms %in% c("bigquery"))
}