library(devtools)

install_github("tomwhite/SqlRender", ref="impala-timestamp")
library(SqlRender)
install_github("ohdsi/Achilles")

library(Achilles)
connectionDetails <- createConnectionDetails(dbms="impala", 
                                             server="localhost",
                                             port=21050, # This port is forwarded on VM (21050 -> 21050)
                                             schema="omop_cdm_parquet",
                                             pathToDriver = "/Users/Maxim/Downloads/impala_drivers/Cloudera_ImpalaJDBC4_2.5.36")

achillesResults <- achilles(connectionDetails, cdmDatabaseSchema="omop_cdm_parquet",
                            resultsDatabaseSchema="achilles", sourceName="Impala trial", runHeel = FALSE,
                            cdmVersion = "5", vocabDatabaseSchema="omop_cdm_parquet", analysisIds = c(1))
