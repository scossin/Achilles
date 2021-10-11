library(Achilles) 
packageVersion("Achilles") # 1.6.7
packageVersion("SqlRender") # 1.8.1

source("./getConnectionDetails.R")

# This issue is discussed here: https://github.com/OHDSI/Achilles/issues/425 
ids_excluded <- c(424, #  ORA-01652: impossible d'étendre le segment temporaire de 256 dans le tablespace TEMP_IAM 
                  624, # ORA-01652 
                  724, # ORA-01652 
                  1824) # ORA-01652
# These queries find and rank the top 10 co-occurring condition (424) / drugs (724) / measurement (1824)
# To do so, these queries perform an inner join on person_id within the same_table
# this inner join takes a lot of memory since these tables contain million of rows
# The limited memory resource in TEMP_IAM (256 go) can't store the results of this inner join
# These queries are not default analysis queries


cdmDatabaseSchema <- "OMOP"
resultsDatabaseSchema <- "OMOP_COHORT"

Achilles::achilles(connectionDetails = connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   resultsDatabaseSchema = resultsDatabaseSchema,
                   scratchDatabaseSchema = resultsDatabaseSchema,
                   vocabDatabaseSchema = cdmDatabaseSchema,
                   oracleTempSchema = resultsDatabaseSchema,
                   createTable = T,
                   smallCellCount = 30,
                   cdmVersion = "5.3.1",
                   runHeel = F, # run achillesHeel after
                   runCostAnalysis = F,
                   createIndices = T,
                   numThreads = 1,
                   dropScratchTables = F,
                   outputFolder = "./output",
                   verboseMode = T,
                   defaultAnalysesOnly = T,
                   optimizeAtlasCache = T # not default  
)


Achilles::achillesHeel(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       resultsDatabaseSchema = resultsDatabaseSchema,
                       scratchDatabaseSchema = resultsDatabaseSchema,
                       oracleTempSchema = resultsDatabaseSchema,
                       vocabDatabaseSchema = cdmDatabaseSchema,
                       cdmVersion = "5.3.1",
                       numThreads = 1,
                       dropScratchTables = T,
                       outputFolder = "output/",
                       verboseMode = T, 
                       sqlOnly = F)

Achilles::dropAllScratchTables(connectionDetails = connectionDetails,
                               scratchDatabaseSchema = resultsDatabaseSchema,
                               outputFolder = "./output",
                               defaultAnalysesOnly = F)


### generate a report to visualize the Achilles results in AchillesWeb application
reports <- Achilles:::getAllReports()
reports_to_ignored <- c("DRUG","DRUG_ERA")  # treemap too long to compute 
reports_json <- reports[!reports %in% reports_to_ignored] 
current_date <- format(Sys.time(), format = "%d%m%Y")
outputPath <- paste0("../AchillesWeb/data/", current_date)
Achilles::exportToJson(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema, 
                       resultsDatabaseSchema = resultsDatabaseSchema,
                       reports = reports_json,
                       outputPath = outputPath)
