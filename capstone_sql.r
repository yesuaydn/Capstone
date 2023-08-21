#install.packages("RSQLite")

#install.packages("readr")

library(RSQLite)
library(readr)

conn <- dbConnect(RSQLite::SQLite())

df1 <- dbExecute(conn, 
                    "CREATE TABLE SEOUL_BIKE_SHARING (
                                      DATE DATE,
                                      RENTED_BIKE_COUNT INTEGER,
                                      HOUR INTEGER,
                                      TEMPERATURE INTEGER,
                                      HUMIDITY INTEGER,
                                      WIND_SPEED INTEGER,
                                      VISIBILITY INTEGER,
                                      DEW_POINT_TEMPERATURE INTEGER,
                                      SOLAR_RADIATION INTEGER,
                                      RAINFALL INTEGER,
                                      SNOWFALL INTEGER,
                                      SEASONS VARCHAR(20),
                                      HOLIDAY VARCHAR(20),
                                      FUNCTIONING_DAY VARCHAR(20)

                                      )", 
                    errors=FALSE
                    )

    if (df1 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

df2 <- dbExecute(conn, 
                    "CREATE TABLE BIKE_SHARING_SYSTEMS (
                                      COUNTRY VARCHAR(20),
                                      CITY VARCHAR(20),
                                      SYSTEM VARCHAR(20),
                                      BICYCLES INTEGER
                                    
                                      )", 
                 
                 
                    errors=FALSE
                    )

    if (df2 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

df3 <- dbExecute(conn, "CREATE TABLE CITIES_WEATHER_FORECAST (
                                  CITY VARCHAR(20),
                                  WEATHER VARCHAR(20),
                                  VISIBILITY INTEGER,
                                  TEMP INTEGER,
                                  TEMP_MIN INTEGER,
                                  TEMP_MAX INTEGER,
                                  PRESSURE INTEGER,
                                  HUMIDITY INTEGER,
                                  WIND_SPEEDINTEGER,
                                  WIND_DEG INTEGER,
                                  SEASON VARCHAR(20),
                                  FORECAST_DATETIME DATE
                            
                                )",
                    errors=FALSE
                    )

    if (df3 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    } 

df4 <- dbExecute(conn, "CREATE TABLE WORLD_CITIES (
                                  CITY VARCHAR(20),
                                  CITY_ASCII VARCHAR(20),
                                  LAT INTEGER,
                                  LNG INTEGER,
                                  COUNTRY VARCHAR(20),
                                  ISO2 VARCHAR(20),
                                  ISO3 VARCHAR(20),
                                  ADMIN_NAME VARCHAR(20),
                                  CAPITAL VARCHAR(20),
                                  POPULATION INTEGER,
                                  ID INTEGER
                                )",
                    errors=FALSE
                    )

    if (df4 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    } 

dbListTables(conn)

seoul_bike_sharing_df <- read_csv("seoul_bike_sharing.csv")
bike_sharing_systems_df <- read_csv("bike_sharing_systems.csv")
cities_weather_forecast_df <- read_csv("cities_weather_forecast.csv")
world_cities_df <- read_csv("world_cities.csv")

dbWriteTable(conn, "SEOUL_BIKE_SHARING", seoul_bike_sharing_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "BIKE_SHARING_SYSTEMS", bike_sharing_systems_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "CITIES_WEATHER_FORECAST", cities_weather_forecast_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "WORLD_CITIES", world_cities_df, overwrite=TRUE, header = TRUE)

dbGetQuery(conn, 'SELECT COUNT(*) FROM SEOUL_BIKE_SHARING')

dbGetQuery(conn, 'SELECT COUNT(HOUR) FROM SEOUL_BIKE_SHARING WHERE RENTED_BIKE_COUNT !=0')

dbGetQuery(conn, 'SELECT * FROM CITIES_WEATHER_FORECAST WHERE CITY = "Seoul" ORDER BY FORECAST_DATETIME LIMIT 1')

dbGetQuery(conn, 'SELECT DISTINCT SEASONS FROM SEOUL_BIKE_SHARING')

dbGetQuery(conn, "
    SELECT MIN(date(substr(DATE, 7, 4) || '-' || substr(DATE, 4, 2) || '-' || substr(DATE, 1, 2))) AS min_date,
           MAX(date(substr(DATE, 7, 4) || '-' || substr(DATE, 4, 2) || '-' || substr(DATE, 1, 2))) AS max_date
    FROM SEOUL_BIKE_SHARING
")

dbGetQuery(conn, 'SELECT DATE, HOUR FROM SEOUL_BIKE_SHARING WHERE RENTED_BIKE_COUNT = (SELECT MAX(RENTED_BIKE_COUNT) FROM SEOUL_BIKE_SHARING)')

dbGetQuery(conn, 'SELECT SEASONS, HOUR % 24 AS HOUR_OF_DAY, AVG(TEMPERATURE) AS avg_temperature, AVG(RENTED_BIKE_COUNT) AS avg_bike_count
                    FROM SEOUL_BIKE_SHARING
                    GROUP BY SEASONS, HOUR_OF_DAY
                    ORDER BY avg_bike_count DESC LIMIT 10')



dbGetQuery(conn, 'SELECT SEASONS,
                    AVG(RENTED_BIKE_COUNT) AS avg_hourly_bike_count, 
                    MIN(RENTED_BIKE_COUNT) AS minimum_hourly_bike_count,
                    MAX(RENTED_BIKE_COUNT) AS maximum_hourly_bike_count,
                    SQRT(AVG(RENTED_BIKE_COUNT*RENTED_BIKE_COUNT) - AVG(RENTED_BIKE_COUNT)*AVG(RENTED_BIKE_COUNT)) AS std_dev_hourly_bike_count
                  FROM SEOUL_BIKE_SHARING
                  GROUP BY SEASONS')

dbGetQuery(conn, 'SELECT SEASONS,
                    AVG(RENTED_BIKE_COUNT) AS avg_bike_count, 
                    AVG(TEMPERATURE) AS avg_temperature, 
                    AVG(HUMIDITY) AS avg_humidity, 
                    AVG(WIND_SPEED) AS avg_wind_speed, 
                    AVG(VISIBILITY) AS avg_visibility, 
                    AVG(DEW_POINT_TEMPERATURE) AS avg_dew_point_temperature, 
                    AVG(SOLAR_RADIATION) AS avg_solar_radiation, 
                    AVG(RAINFALL) AS avg_rainfall, 
                    AVG(SNOWFALL) AS avg_snowfall
                  FROM SEOUL_BIKE_SHARING
                  GROUP BY SEASONS
                  ORDER BY avg_bike_count')


dbGetQuery(conn, 'SELECT SUM(BSS.BICYCLES) AS total_bicycles, WC.CITY_ASCII, WC.COUNTRY, WC.LAT, WC.LNG, WC.POPULATION 
                    FROM WORLD_CITIES WC, BIKE_SHARING_SYSTEMS BSS
                    WHERE WC.CITY_ASCII = BSS.CITY AND BSS.CITY = "Seoul"')

dbGetQuery(conn, 'SELECT SUM(BSS.BICYCLES) AS total_bicycles, WC.CITY, WC.COUNTRY, WC.LAT, WC.LNG, WC.POPULATION 
                    FROM WORLD_CITIES WC, BIKE_SHARING_SYSTEMS BSS
                    WHERE  WC.CITY_ASCII = BSS.CITY 
                    GROUP BY WC.CITY
                    HAVING total_bicycles BETWEEN 15000 AND 20000')

seoul_bike_sharing <- read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-RP0321EN-SkillsNetwork/labs/datasets/seoul_bike_sharing.csv")  

class(seoul_bike_sharing$DATE)

head(seoul_bike_sharing, n=2)

any(is.na(seoul_bike_sharing$DATE))

dbGetQuery(conn, "SELECT STRFTIME('%d/%m/%Y', DATE) FROM seoul_bike_sharing")


