# Note: when switching between tutorial examples,
# restart R by clicking Session | Restart R in RStudio

setwd("~/KDD2017R/Code/MRS")
source("SetComputeContext.r")

library(sparklyr)
library(dplyr)

Sys.setenv(SPARK_VERSION="2.1.1")

cc <- rxSparkConnect(interop = "sparklyr",
                     reset = TRUE,
                     consoleOutput = TRUE,
                     # numExecutors = 1,
                     executorCores = 4,
                     driverMem = "2g",
                     executorMem = "2g",
                     executorOverheadMem = "4g"
)

sc <- rxGetSparklyrConnection(cc)


################################################
# Specify the data sources
################################################


airlineDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "airline",
                                      path = file.path(dataDir, "AirlineSubsetCsv"), 
                                      header = TRUE, 
                                      infer_schema = FALSE, # Avoids parsing error
                                      null_value = "null")

weatherDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "weather",
                                      path = file.path(dataDir, "WeatherSubsetCsv"), 
                                      header = TRUE,
                                      infer_schema = TRUE,
                                      null_value = "null")


################################################
# Transform the data
################################################

airlineDF <- airlineDF %>%
  rename(ArrDel15 = ARR_DEL15) %>%
  rename(Year = YEAR) %>%
  rename(Month = MONTH) %>%
  rename(DayOfMonth = DAY_OF_MONTH) %>%
  rename(DayOfWeek = DAY_OF_WEEK) %>%
  rename(Carrier = UNIQUE_CARRIER) %>%
  rename(OriginAirportID = ORIGIN_AIRPORT_ID) %>%
  rename(DestAirportID = DEST_AIRPORT_ID) %>%
  rename(CRSDepTime = CRS_DEP_TIME) %>%
  rename(CRSArrTime = CRS_ARR_TIME)


# Keep only the desired columns from the flight data 

airlineDF <- airlineDF %>% select(ArrDel15, Year, Month, DayOfMonth, 
                    DayOfWeek, Carrier, OriginAirportID, 
                    DestAirportID, CRSDepTime, CRSArrTime)


# Round down scheduled departure time to full hour

airlineDF <- airlineDF %>% mutate(CRSDepTime = floor(CRSDepTime / 100))


weatherDF <- weatherDF %>%
  rename(OriginAirportID = AirportID) %>%
  rename(Year = AdjustedYear) %>%
  rename(Month = AdjustedMonth) %>%
  rename(DayOfMonth = AdjustedDay) %>%
  rename(CRSDepTime = AdjustedHour)


# Average the weather readings by hour

weatherSummary <- weatherDF %>% 
  group_by(Year, Month, DayOfMonth, CRSDepTime, OriginAirportID) %>% 
  summarise(Visibility = mean(Visibility),
            DryBulbCelsius = mean(DryBulbCelsius),
            DewPointCelsius = mean(DewPointCelsius),
            RelativeHumidity = mean(RelativeHumidity),
            WindSpeed = mean(WindSpeed),
            Altimeter = mean(Altimeter))


#######################################################
# Join airline data with weather at Origin Airport
#######################################################

originDF <- left_join(x = airlineDF,
                      y = weatherSummary)

originDF <- originDF %>%
  rename(VisibilityOrigin = Visibility) %>%
  rename(DryBulbCelsiusOrigin = DryBulbCelsius) %>%
  rename(DewPointCelsiusOrigin = DewPointCelsius) %>%
  rename(RelativeHumidityOrigin = RelativeHumidity) %>%
  rename(WindSpeedOrigin = WindSpeed) %>%
  rename(AltimeterOrigin = Altimeter)


#######################################################
# Join airline data with weather at Destination Airport
#######################################################

weatherSummary <- weatherSummary %>% rename(DestAirportID = OriginAirportID)

destDF <- left_join(x = originDF,
                    y = weatherSummary)

airWeatherDF <- destDF %>%
  rename(VisibilityDest = Visibility) %>%
  rename(DryBulbCelsiusDest = DryBulbCelsius) %>%
  rename(DewPointCelsiusDest = DewPointCelsius) %>%
  rename(RelativeHumidityDest = RelativeHumidity) %>%
  rename(WindSpeedDest = WindSpeed) %>%
  rename(AltimeterDest = Altimeter)


#######################################################
# Register the joined data as a Spark SQL/Hive table
#######################################################

# NOTE: IGNORE "Translator is missing window functions" WARNING
# https://github.com/rstudio/sparklyr/issues/792
airWeatherDF <- airWeatherDF %>% sdf_register("flightsweather")

tbl_cache(sc, "flightsweather")


#######################################################
# The table of joined data can be queried using SQL
#######################################################

# Count the number of rows
tbl(sc, sql("SELECT COUNT(*) FROM flightsweather"))

# Count each distinct value in the ArrDel15 column
tbl(sc, sql("SELECT ArrDel15, COUNT(*) FROM flightsweather GROUP BY ArrDel15"))

# Count rows by Year and Month
tbl(sc, sql("SELECT Year, Month, COUNT(*) FROM flightsweather GROUP BY Year, Month ORDER BY Year, Month"))

