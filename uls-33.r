# uls-xx.r
# R file to extract, transform, and load (ETL) the FCC Universal Licensing System (ULS) files, and manage/convert them appropriately for end-users


# wayne.smith@csun.edu
# created: Jul 22, 2002
# updated: May 20, 2020


# prepended object identifiers
# df = data frame (list)
# f = function
# l = list (recursive vector)
# s = scalar (one-element vector)
# s4 = generic s4 object
# time = time (POSIXct)
# v = vector (multi-element vector)


# we need the 'downloader' package (to get the files from the FCC ULS server) -- '> install.packages( "downloader" )'
# (...without having to worry about the underlying 'download.file' method...)
require( "downloader" )

# we need the 'DBI' package (to provide a consistent, DBMS-neutral Databse access environment) -- '> install.packages( "DBI" )'
require( "DBI" )

# we need the 'RSQLite' package (to manage the row-oriented relational DBMS structure) -- '> install.packages( "RSQLite" )'
require( "RSQLite" )

# we need the 'stringr' package (to help with string pattern matching without resorting to Regular Expressions) -- '> install.packages( "stringr" )'
require( "stringr" )

# we need the 'data.table' package (to help write .csv files faster) -- '> install.packages( "data.table" )'
# but it writes a file that can't be imported correctly...e.g., into MS-Access (so I'm temporarily not using it)
require( "data.table" )

# we need the "openxlsx" package -- '> install.packages( "openxlsx" )'
# this helps with writing Excel files
library( "openxlsx" )


# Post-Join Queries (US)
# --------------------------------------------------
f.dbmspostjoinqueriesUS <- function( s4.db ) {

  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "indexing the 'US' table", "..." ))

  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX frequency_assigned_tblGeoUS_index ON tblGeoUS (frequency_assigned)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_tblGeoUS_index ON tblGeoUS (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX radio_service_code_tblGeoUS_index ON tblGeoUS (radio_service_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX class_station_code_tblGeoUS_index ON tblGeoUS (class_station_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_county_tblGeoUS_index ON tblGeoUS (location_county)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_city_tblGeoUS_index ON tblGeoUS (location_city)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_address_tblGeoUS_index ON tblGeoUS (location_address)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX entity_name_tblGeoUS_index ON tblGeoUS (entity_name)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX control_county_tblGeoUS_index ON tblGeoUS (control_county)" )

  print( Sys.time() - time.tic )

  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "exporting the 'US' table", "..." ))

  # old way (works)
#  df.res <- dbGetQuery( conn = s4.db,
#    "SELECT * FROM tblGeoUS
#    "
#  )
  # new way (also works, slightly faster)
  df.res <- dbReadTable( conn = s4.db, name = "tblGeoUS" )

  # make sure it is ordered correctly
  df.res <- df.res[
    order(
      df.res$frequency_assigned,
      df.res$call_sign,
      df.res$radio_service_code,
      df.res$class_station_code,
      df.res$location_state,
      df.res$location_county,
      df.res$location_city,
      df.res$location_address
    ),
  ]

#  fwrite( x = df.res, file = "us.csv", sep = "|", row.names = FALSE )
  write.table( x = df.res, file = "us.csv", sep = "|", row.names = FALSE )

  print( Sys.time() - time.tic )

  rm( df.res )


  return()

}


# Post-Join Queries (CA)
# --------------------------------------------------
f.dbmspostjoinqueriesCA <- function( s4.db ) {

  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "indexing the final 'CA' table", "..." ))

  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX frequency_assigned_tblGeoStateCA_index ON tblGeoStateCA (frequency_assigned)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_tblGeoStateCA_index ON tblGeoStateCA (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX radio_service_code_tblGeoStateCA_index ON tblGeoStateCA (radio_service_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX class_station_code_tblGeoStateCA_index ON tblGeoStateCA (class_station_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_county_tblGeoStateCA_index ON tblGeoStateCA (location_county)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_city_tblGeoStateCA_index ON tblGeoStateCA (location_city)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_address_tblGeoStateCA_index ON tblGeoStateCA (location_address)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX entity_name_tblGeoStateCA_index ON tblGeoStateCA (entity_name)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX control_county_tblGeoStateCA_index ON tblGeoStateCA (control_county)" )

  print( Sys.time() - time.tic )

  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "generating the 'SoCal' (subsetted from 'CA') table", "..." ))

  s.rows <- dbExecute( conn = s4.db,
  "CREATE TABLE tblGeoCountiesSoCal
  AS SELECT DISTINCT *

  FROM tblGeoStateCA

  WHERE (
         location_county='IMPERIAL' OR
         location_county='KERN' OR
         location_county='LOS ANGELES' OR
         location_county='ORANGE' OR
         location_county='RIVERSIDE' OR
         location_county='SAN BERNARDINO' OR
         location_county='SAN DIEGO' OR
         location_county='SAN LUIS OBISPO' OR
         location_county='SANTA BARBARA' OR
         location_county='VENTURA'
        )
         OR

        (
         (location_county='' OR location_county IS NULL) AND (
         control_county='IMPERIAL' OR
         control_county='KERN' OR
         control_county='LOS ANGELES' OR
         control_county='ORANGE' OR
         control_county='RIVERSIDE' OR
         control_county='SAN BERNARDINO' OR
         control_county='SAN DIEGO' OR
         control_county='SAN LUIS OBISPO' OR
         control_county='SANTA BARBARA' OR
         control_county='VENTURA')
        )

  ORDER BY frequency_assigned,
      call_sign,
      radio_service_code,
      class_station_code,
      location_state,
      location_county,
      location_city,
      location_address
    "
  )
  print( Sys.time() - time.tic )

  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "exporting the 'CA' table", "..." ))

  # old way (works)
#  df.res <- dbGetQuery( conn = s4.db,
#    "SELECT * FROM tblGeoStateCA
#    "
#  )
  # new way (also works, slightly faster)
  df.res <- dbReadTable( conn = s4.db, name = "tblGeoStateCA" )

  # make sure it is ordered correctly
  df.res <- df.res[
    order(
      df.res$frequency_assigned,
      df.res$call_sign,
      df.res$radio_service_code,
      df.res$class_station_code,
      df.res$location_state,
      df.res$location_county,
      df.res$location_city,
      df.res$location_address
    ),
  ]

#  fwrite( x = df.res, file = "ca.csv", sep = "|", row.names = FALSE )
  write.table( x = df.res, file = "ca.csv", sep = "|", row.names = FALSE )

  print( Sys.time() - time.tic )

  rm( df.res )


  # document the progress to the user
  time.tic <- Sys.time()
  print( paste0( "exporting the 'CA SoCal' table", "..." ))

  # old way (works)
#  df.res <- dbGetQuery( conn = s4.db,
#    "SELECT * FROM tblGeoCountiesSoCal
#    "
#  )
  # new way (also works, slightly faster)
  df.res <- dbReadTable( conn = s4.db, name = "tblGeoCountiesSoCal" )

  # make sure it is ordered correctly
  df.res <- df.res[
    order(
      df.res$frequency_assigned,
      df.res$call_sign,
      df.res$radio_service_code,
      df.res$class_station_code,
      df.res$location_state,
      df.res$location_county,
      df.res$location_city,
      df.res$location_address
    ),
  ]

  fwrite( x = df.res, file = "socal.csv", sep = "|", row.names = FALSE )
#  write.table( x = df.res, file = "socal.csv", sep = "|", row.names = FALSE )

  print( Sys.time() - time.tic )


  # generate files for individual CA SoCal counties

  # set which counties we want (currently, CA SoCal counties only)
  v.socal.counties <- c(
   "IMPERIAL", "KERN", "LOS ANGELES", "ORANGE", "RIVERSIDE",
   "SAN BERNARDINO", "SAN DIEGO", "SAN LUIS OBISPO", "SANTA BARBARA", "VENTURA"
  )

  # loop through each CA SoCal county
  for( s.counter in 1: length( v.socal.counties )) {

    # get the current county
    s.socal.county <- v.socal.counties[ s.counter ]

    # document the progress to the user
    print( paste0( "exporting the 'CA SoCal'", " -- ", s.socal.county, " (subsetted from 'CA SoCal')", " table", "..." ))

    # extract the data just for that county (from the SoCalCounties dataframe)
#    df.res.county <- df.res[ location_county == s.socal.county | (( location_county == "" | location_county == NULL ) & control_county == s.socal.county ), ]
#    df.res.county <- df.res[ location_county == s.socal.county | (( location_county == "" | is.na( location_county )) & control_county == s.socal.county ), ]
#    df.res.county <- df.res[ df.res$location_county == s.socal.county | df.res$control_county == s.socal.county, ]
    df.res.county <- subset( x = df.res, subset = ( location_county == s.socal.county | control_county == s.socal.county ))

    # we only want the "Active" licenses
    df.res.county <- subset( x = df.res.county, subset = license_status == "A" )

    # we don't want a few columns
    df.res.county <- subset( x = df.res.county, select = -c( license_status, frequency_upper_band, frequency_carrier ))

    # remove any spaces within the county name
    s.socal.county.nospaces <- gsub( pattern = " ", replacement = "", x = s.socal.county )

    # and convert the county name to lower case
    s.socal.county.nospaces <- tolower( x = s.socal.county.nospaces )

    # write the file out as a .csv
#    fwrite( x = df.res.county, file = paste0( "ca-", s.socal.county.nospaces, ".csv" ), sep = "|", row.names = FALSE )
    write.table( x = df.res.county, file = paste0( "ca-", s.socal.county.nospaces, ".csv" ), sep = "|", row.names = FALSE )


    # create an Excel Workbook
    s4.workbook <- createWorkbook()

    # create a Worksheet within the Excel Workbook
    s.sheetname <- s.socal.county.nospaces
    s4.worksheet <- addWorksheet( wb = s4.workbook, sheetName = s.sheetname )

    # add data to the Worksheet within the Excel Workbook
    s4.writedata <- writeData( wb = s4.workbook, sheet = s.sheetname, x = df.res.county, rowNames = FALSE )

    # save the workbook
    s4.saveworkbook <- saveWorkbook( wb = s4.workbook, file = paste0( "ca-", s.socal.county.nospaces, ".xlsx" ), overwrite = TRUE )

    rm( s4.worksheet )
    rm( s4.workbook )


    rm( df.res.county )

  }


  rm( df.res )


  return()

}


# Join Query (US)
# --------------------------------------------------
f.dbmsjoinqueryUS <- function( s4.db ) {

  # set important constants

  # document the progress to the user
  print( paste0( "generating the 'US' (joined) table", "..." ))
  time.tic <- Sys.time()

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE IF EXISTS tblGeoUS
    "
  )


  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE tblGeoUS
  AS SELECT DISTINCT
      PUBACC_FR.frequency_assigned            ,
      PUBACC_FR.call_sign                     ,

      PUBACC_HD.license_status                ,
      PUBACC_HD.radio_service_code            ,

      PUBACC_FR.class_station_code            ,

      PUBACC_LO.location_state                ,
      PUBACC_LO.location_county               ,
      PUBACC_LO.location_city                 ,
      PUBACC_LO.location_address              ,
      PUBACC_LO.location_name                 ,

      PUBACC_EN.entity_name                   ,
      PUBACC_EN.state                         ,
      PUBACC_EN.city                          ,
      PUBACC_EN.street_address                ,

      PUBACC_CP.state_code                    ,
      PUBACC_CP.control_county                ,
      PUBACC_CP.control_city                  ,
      PUBACC_CP.control_address               ,
      PUBACC_CP.control_phone                 ,

      PUBACC_FR.cnt_mobile_units              ,

      PUBACC_HD.grant_date                    ,
      PUBACC_HD.expired_date                  ,
      PUBACC_HD.cancellation_date             ,
      PUBACC_HD.effective_date                ,
      PUBACC_HD.last_action_date              ,
      PUBACC_FR.unique_system_identifier      ,
      PUBACC_FR.ULS_File_Number               ,

      PUBACC_LO.ground_elevation              ,
      PUBACC_LO.lat_degrees                   ,
      PUBACC_LO.lat_minutes                   ,
      PUBACC_LO.lat_seconds                   ,
      PUBACC_LO.lat_direction                 ,
      PUBACC_LO.long_degrees                  ,
      PUBACC_LO.long_minutes                  ,
      PUBACC_LO.long_seconds                  ,
      PUBACC_LO.long_direction                ,

      PUBACC_FR.frequency_upper_band          ,
      PUBACC_FR.frequency_carrier             

--      PUBACC_FR.db_id

  FROM PUBACC_LO

        LEFT JOIN PUBACC_CP ON PUBACC_LO.call_sign=PUBACC_CP.call_sign
        INNER JOIN PUBACC_EN ON PUBACC_LO.call_sign=PUBACC_EN.call_sign
        INNER JOIN PUBACC_HD ON PUBACC_LO.call_sign=PUBACC_HD.call_sign
        INNER JOIN PUBACC_FR ON PUBACC_LO.call_sign=PUBACC_FR.call_sign

  WHERE (PUBACC_EN.entity_type = 'L')

  ORDER BY PUBACC_FR.frequency_assigned,
      PUBACC_FR.call_sign,
      PUBACC_HD.license_status,
      PUBACC_HD.radio_service_code,
      PUBACC_FR.class_station_code,
      PUBACC_LO.location_state,
      PUBACC_LO.location_county,
      PUBACC_LO.location_city,
      PUBACC_LO.location_address
    "
  )
  print( Sys.time() - time.tic )


  return()

}


# Join Query (CA)
# --------------------------------------------------
f.dbmsjoinqueryCA <- function( s4.db ) {

  # set important constants

  # document the progress to the user
  print( paste0( "generating the initial 'CA' (joined) table", "..." ))

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE IF EXISTS tblGeoStateCA1
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE IF EXISTS tblGeoStateCA2
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE IF EXISTS tblGeoStateCA3
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE tblGeoStateCA1
       AS SELECT DISTINCT
            PUBACC_EN.call_sign                     ,
            PUBACC_EN.entity_name AS 'contact_licensee'
              FROM PUBACC_EN
          WHERE (PUBACC_EN.entity_type = 'CL')
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "CREATE INDEX call_sign_tblGeoStateCA1_index ON PUBACC_CP (call_sign)
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE IF EXISTS tblGeoStateCA
    "
  )


  # do the main join
  # it appears that there is a hard max. of 6 tables (so we need to do two joins in sequence rather than just one big join)
  time.tic <- Sys.time()
  print( paste0( "generating the main 'CA' join", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE tblGeoStateCA2
  AS SELECT DISTINCT
      PUBACC_FR.frequency_assigned            ,
      PUBACC_FR.call_sign                     ,

      PUBACC_HD.license_status                ,
      PUBACC_HD.radio_service_code            ,

      PUBACC_FR.class_station_code            ,

      PUBACC_LO.location_state                ,
      PUBACC_LO.location_county               ,
      PUBACC_LO.location_city                 ,
      PUBACC_LO.location_address              ,
      PUBACC_LO.location_name                 ,

      tblGeoStateCA1.contact_licensee         ,

      PUBACC_EN.entity_name                   ,
      PUBACC_EN.state                         ,
      PUBACC_EN.city                          ,
      PUBACC_EN.street_address                ,

      PUBACC_CP.state_code                    ,
      PUBACC_CP.control_county                ,
      PUBACC_CP.control_city                  ,
      PUBACC_CP.control_address               ,
      PUBACC_CP.control_phone                 ,

      PUBACC_FR.cnt_mobile_units              ,

      PUBACC_HD.grant_date                    ,
      PUBACC_HD.expired_date                  ,
      PUBACC_HD.cancellation_date             ,
      PUBACC_HD.effective_date                ,
      PUBACC_HD.last_action_date              ,
      PUBACC_FR.unique_system_identifier      ,
      PUBACC_FR.ULS_File_Number               ,

      PUBACC_LO.ground_elevation              ,
      PUBACC_LO.lat_degrees                   ,
      PUBACC_LO.lat_minutes                   ,
      PUBACC_LO.lat_seconds                   ,
      PUBACC_LO.lat_direction                 ,
      PUBACC_LO.long_degrees                  ,
      PUBACC_LO.long_minutes                  ,
      PUBACC_LO.long_seconds                  ,
      PUBACC_LO.long_direction                ,

      PUBACC_FR.frequency_upper_band          ,
      PUBACC_FR.frequency_carrier             

--      PUBACC_FR.db_id

  FROM PUBACC_LO

        LEFT JOIN PUBACC_CP ON PUBACC_LO.call_sign=PUBACC_CP.call_sign
        INNER JOIN PUBACC_EN ON PUBACC_LO.call_sign=PUBACC_EN.call_sign
        INNER JOIN PUBACC_HD ON PUBACC_LO.call_sign=PUBACC_HD.call_sign
        INNER JOIN PUBACC_FR ON PUBACC_LO.call_sign=PUBACC_FR.call_sign
        LEFT JOIN tblGeoStateCA1 ON PUBACC_LO.call_sign=tblGeoStateCA1.call_sign

  WHERE (PUBACC_EN.entity_type = 'L') AND
        (location_state = 'CA' OR state = 'CA' OR state_code = 'CA')

--  ORDER BY PUBACC_FR.frequency_assigned,
--      PUBACC_FR.call_sign,
--      PUBACC_HD.license_status,
--      PUBACC_HD.radio_service_code,
--      PUBACC_FR.class_station_code,
--      PUBACC_LO.location_state,
--      PUBACC_LO.location_county,
--      PUBACC_LO.location_city,
--      PUBACC_LO.location_address
    "
  )

  print( Sys.time() - time.tic )

  # create indexes (tblGeoStateCA2)
  print( paste0( "indexing ", "tblGeoStateCA2", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_tblGeoStateCA2_index ON tblGeoStateCA2 (call_sign)" )

  time.tic <- Sys.time()
  print( paste0( "generating the 'Emissions' join", "..." ))
  s.rows <- dbExecute( conn = s4.db,
#    "EXPLAIN QUERY PLAN CREATE TABLE tblGeoStateCA3
    "CREATE TABLE tblGeoStateCA3
  AS SELECT DISTINCT
      tblGeoStateCA2.frequency_assigned       ,
      tblGeoStateCA2.call_sign                ,

      tblGeoStateCA2.license_status           ,
      tblGeoStateCA2.radio_service_code       ,

      tblGeoStateCA2.class_station_code       ,

      tblGeoStateCA2.location_state           ,
      tblGeoStateCA2.location_county          ,
      tblGeoStateCA2.location_city            ,
      tblGeoStateCA2.location_address         ,
      tblGeoStateCA2.location_name            ,

      tblGeoStateCA2.contact_licensee         ,

      tblGeoStateCA2.entity_name              ,
      tblGeoStateCA2.state                    ,
      tblGeoStateCA2.city                     ,
      tblGeoStateCA2.street_address           ,

      tblGeoStateCA2.state_code               ,
      tblGeoStateCA2.control_county           ,
      tblGeoStateCA2.control_city             ,
      tblGeoStateCA2.control_address          ,
      tblGeoStateCA2.control_phone            ,

      tblGeoStateCA2.cnt_mobile_units         ,

      tblGeoStateCA2.grant_date               ,
      tblGeoStateCA2.expired_date             ,
      tblGeoStateCA2.cancellation_date        ,
      tblGeoStateCA2.effective_date           ,
      tblGeoStateCA2.last_action_date         ,
      tblGeoStateCA2.unique_system_identifier ,
      tblGeoStateCA2.ULS_File_Number          ,

      tblGeoStateCA2.ground_elevation         ,
      tblGeoStateCA2.lat_degrees              ,
      tblGeoStateCA2.lat_minutes              ,
      tblGeoStateCA2.lat_seconds              ,
      tblGeoStateCA2.lat_direction            ,
      tblGeoStateCA2.long_degrees             ,
      tblGeoStateCA2.long_minutes             ,
      tblGeoStateCA2.long_seconds             ,
      tblGeoStateCA2.long_direction           ,

      PUBACC_EM.emission_code                 ,

      tblGeoStateCA2.frequency_upper_band     ,
      tblGeoStateCA2.frequency_carrier        

--      PUBACC_FR.db_id

  FROM tblGeoStateCA2

        LEFT JOIN PUBACC_EM ON tblGeoStateCA2.call_sign=PUBACC_EM.call_sign

--  ORDER BY tblGeoStateCA2.frequency_assigned,
--      tblGeoStateCA2.call_sign,
--      tblGeoStateCA2.license_status,
--      tblGeoStateCA2.radio_service_code,
--      tblGeoStateCA2.class_station_code,
--      tblGeoStateCA2.location_state,
--      tblGeoStateCA2.location_county,
--      tblGeoStateCA2.location_city,
--      tblGeoStateCA2.location_address
    "
  )
#g.res <<- res
#stop()

  print( Sys.time() - time.tic )

  time.tic <- Sys.time()
  print( paste0( "sorting the final 'CA' table", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE tblGeoStateCA
  AS SELECT DISTINCT
      tblGeoStateCA3.frequency_assigned       ,
      tblGeoStateCA3.call_sign                ,

      tblGeoStateCA3.license_status           ,
      tblGeoStateCA3.radio_service_code       ,

      tblGeoStateCA3.class_station_code       ,

      tblGeoStateCA3.location_state           ,
      tblGeoStateCA3.location_county          ,
      tblGeoStateCA3.location_city            ,
      tblGeoStateCA3.location_address         ,
      tblGeoStateCA3.location_name            ,

      tblGeoStateCA3.contact_licensee         ,

      tblGeoStateCA3.entity_name              ,
      tblGeoStateCA3.state                    ,
      tblGeoStateCA3.city                     ,
      tblGeoStateCA3.street_address           ,

      tblGeoStateCA3.state_code               ,
      tblGeoStateCA3.control_county           ,
      tblGeoStateCA3.control_city             ,
      tblGeoStateCA3.control_address          ,
      tblGeoStateCA3.control_phone            ,

      tblGeoStateCA3.cnt_mobile_units         ,

      tblGeoStateCA3.grant_date               ,
      tblGeoStateCA3.expired_date             ,
      tblGeoStateCA3.cancellation_date        ,
      tblGeoStateCA3.effective_date           ,
      tblGeoStateCA3.last_action_date         ,
      tblGeoStateCA3.unique_system_identifier ,
      tblGeoStateCA3.ULS_File_Number          ,

      tblGeoStateCA3.ground_elevation         ,
      tblGeoStateCA3.lat_degrees              ,
      tblGeoStateCA3.lat_minutes              ,
      tblGeoStateCA3.lat_seconds              ,
      tblGeoStateCA3.lat_direction            ,
      tblGeoStateCA3.long_degrees             ,
      tblGeoStateCA3.long_minutes             ,
      tblGeoStateCA3.long_seconds             ,
      tblGeoStateCA3.long_direction           ,

      tblGeoStateCA3.emission_code            ,

      tblGeoStateCA3.frequency_upper_band     ,
      tblGeoStateCA3.frequency_carrier        

--      PUBACC_FR.db_id

  FROM tblGeoStateCA3

--        LEFT JOIN PUBACC_EM ON tblGeoStateCA2.call_sign=PUBACC_EM.call_sign

  ORDER BY tblGeoStateCA3.frequency_assigned,
      tblGeoStateCA3.call_sign,
      tblGeoStateCA3.license_status,
      tblGeoStateCA3.radio_service_code,
      tblGeoStateCA3.class_station_code,
      tblGeoStateCA3.location_state,
      tblGeoStateCA3.location_county,
      tblGeoStateCA3.location_city,
      tblGeoStateCA3.location_address
    "
  )

  print( Sys.time() - time.tic )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE tblGeoStateCA3
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE tblGeoStateCA2
    "
  )

  s.rows <- dbExecute( conn = s4.db,
    "DROP TABLE tblGeoStateCA1
    "
  )

  return()

}


# Create Indexes
# --------------------------------------------------
f.dbmsindexes <- function( s4.db ) {

  # set important constants


  # document the progress to the user
#  print( paste0( "indexing ", "..." ))


  # create indexes (CP)
  print( paste0( "indexing ", "CP", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_CP_index ON PUBACC_CP (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX state_code_CP_index ON PUBACC_CP (state_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX control_county_CP_index ON PUBACC_CP (control_county)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX control_city_CP_index ON PUBACC_CP (control_city)" )

  # create indexes (EM)
  print( paste0( "indexing ", "EM", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_EM_index ON PUBACC_EM (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX emission_code_EM_index ON PUBACC_EM (emission_code)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX emission_code2_EM_index ON PUBACC_EM (call_sign, emission_code)" )

  # create indexes (EN)
  print( paste0( "indexing ", "EN", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_EN_index ON PUBACC_EN (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX state_EN_index ON PUBACC_EN (state)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX city_EN_index ON PUBACC_EN (city)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX entity_type_EN_index ON PUBACC_EN (entity_type)" )

  # create indexes (FR)
  print( paste0( "indexing ", "FR", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_FR_index ON PUBACC_FR (call_sign)" )

  # create indexes (HD)
  print( paste0( "indexing ", "HD", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_HD_index ON PUBACC_HD (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX grant_date_HD_index ON PUBACC_HD (grant_date)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX expired_date_HD_index ON PUBACC_HD (expired_date)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX cancellation_HD_date_index ON PUBACC_HD (cancellation_date)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX effective_date_HD_index ON PUBACC_HD (effective_date)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX last_action_date_HD_index ON PUBACC_HD (last_action_date)" )

  # create indexes (HS)
  print( paste0( "indexing ", "HS", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX unique_system_identifier_HS_index ON PUBACC_HS (unique_system_identifier)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX ULS_File_Number_HS_index ON PUBACC_HS (ULS_File_Number)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_HS_index ON PUBACC_HS (call_sign)" )

  # create indexes (LO)
  print( paste0( "indexing ", "LO", "..." ))
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX call_sign_LO_index ON PUBACC_LO (call_sign)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_state_LO_index ON PUBACC_LO (location_state)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_county_LO_index ON PUBACC_LO (location_county)" )
  s.rows <- dbExecute( conn = s4.db, "CREATE INDEX location_city_LO_index ON PUBACC_LO (location_city)" )


  return()

}


# Imputation Queries
# --------------------------------------------------
f.dbmsimputationqueries <- function( s4.db ) {

  # imputing City from Address
  print( paste0( "imputing City from Address", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "UPDATE pubacc_lo
      SET location_city = 'PASADENA'
        WHERE (location_city = '') AND (
        location_address = 'MT. WILSON ANTENNA FARM'
    "
  )


  # imputing State from County
  print( paste0( "imputing State from County", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "UPDATE pubacc_lo
  SET location_state = 'CA'
  WHERE (location_county <> 'CA') AND (
        location_county = 'ALAMEDA' OR
        location_county = 'ALPINE' OR
        location_county = 'AMADOR' OR
        location_county = 'BUTTE' OR
        location_county = 'CALAVERAS' OR
        location_county = 'COLUSA' OR
        location_county = 'CONTRA COSTA' OR
        location_county = 'DEL NORTE' OR
        location_county = 'EL DORADO' OR
        location_county = 'FRESNO' OR
        location_county = 'GLENN' OR
        location_county = 'HUMBOLDT' OR
        location_county = 'IMPERIAL' OR
        location_county = 'INYO' OR
        location_county = 'KERN' OR
        (location_county = 'KINGS' AND
          (
            location_city = 'ARMONA' OR location_city = 'AVENA' OR location_city = 'AVENAL' OR
            location_city = 'BLACKWELLS CORNER' OR location_city = 'COALINGA' OR location_city = 'COCORAN' OR
            location_city = 'CORCORAN' OR location_city = 'GUERNSEY' OR location_city = 'HANFORD' OR
            location_city = 'KETTLEMAN' OR location_city = 'KETTLEMAN CITY' OR location_city = 'LEMOORE' OR
            location_city = 'LEMOORE NAS' OR location_city = 'NAS LEMOORE' OR location_city = 'SHIRLEY'
          )
        ) OR
        (location_county = 'LAKE' AND
          (
            location_city = 'ADAMS' OR location_city = 'ANDERSON SPRINGS' OR location_city = 'BLUE LAKES' OR
            location_city = 'CLEAR LAKE' OR location_city = 'CLEAR LAKE RIVERA' OR location_city = 'CLEARLAKE' OR
            location_city = 'CLEARLAKE CITY' OR location_city = 'CLEARLAKE HIGHLANDS' OR location_city = 'CLEARLAKE OAKS' OR
            location_city = 'CLEARLAKE PARK' OR location_city = 'COBB' OR location_city = 'FOREST LAKE' OR
            location_city = 'GLENHAVEN' OR location_city = 'HOBERGS' OR location_city = 'KELSEYVILLE' OR
            location_city = 'LAKE PORT' OR location_city = 'LAKEPORT' OR location_city = 'LOCH LOMOND' OR
            location_city = 'LOWER LAKE' OR location_city = 'LOWERLAKE' OR location_city = 'LUCERNE' OR
            location_city = 'MIDDLERTOWN' OR location_city = 'MIDDLETON' OR location_city = 'MIDDLETOWN' OR
            location_city = 'NEAREST IS KELSEYVIL' OR location_city = 'NICE' OR location_city = 'PINE GROVE' OR
            location_city = 'UKIAH' OR location_city = 'UPPER LAKE' OR location_city = 'WALNUT CREEK' OR
            location_city = 'WILLITS'
          )
        ) OR
        location_county = 'LASSEN' OR
        location_county = 'LOS ANGELES' OR
        location_county = 'MADERA' OR
        location_county = 'MARIN' OR
        location_county = 'MARIPOSA' OR
        location_county = 'MENDOCINO' OR
        location_county = 'MERCED' OR
        location_county = 'MODOC' OR
        location_county = 'MONO' OR
        location_county = 'MONTEREY' OR
        location_county = 'NAPA' OR
        location_county = 'NEVADA' OR
        (location_county = 'ORANGE' AND
          (
            location_city = '3111 E CHAPMAN AVE.' OR location_city = 'ALISO VIEGO' OR location_city = 'ALISO VIEJO' OR
            location_city = 'ANAHEIM' OR location_city = 'ANAHEIM HILLS' OR location_city = 'ANAHIEM' OR
            location_city = 'ATWOOD' OR location_city = 'BALBOA' OR location_city = 'BALBOA ISLAND' OR
            location_city = 'BATAVIA' OR location_city = 'BOLERO PEAK' OR location_city = 'BREA' OR
            location_city = 'BUENA PARK' OR location_city = 'CERRITOS' OR location_city = 'CHINO' OR
            location_city = 'CHINO HILLS' OR location_city = 'CLEMENTE' OR location_city = 'COLTON' OR
            location_city = 'COMMECE' OR location_city = 'CORNING' OR location_city = 'CORNOA' OR
            location_city = 'CORONA' OR location_city = 'CORONA DEL MAR' OR location_city = 'COSTA  MESA' OR
            location_city = 'COSTA MESA' OR location_city = 'COTO DE COZA' OR location_city = 'CYPRESS' OR
            location_city = 'DANA POINT' OR location_city = 'EL MODENA' OR location_city = 'EL TORO' OR
            location_city = 'ELTORO' OR location_city = 'EMERALD BAY' OR location_city = 'FOUNTAIN VALLEY' OR
            location_city = 'FULLERTON' OR location_city = 'GARDEN GROVE' OR location_city = 'HUNTINGTON' OR
            location_city = 'HUNTINGTON BCH' OR location_city = 'HUNTINGTON BEACH' OR location_city = 'IRVINE' OR
            location_city = 'JAMISON SPRING' OR location_city = 'LA HABRA' OR location_city = 'LA HABRA HEIGHTS' OR
            location_city = 'LA MIRADA' OR location_city = 'LA PALMA' OR location_city = 'LADERA RANCH' OR
            location_city = 'LAGUNA BEACH' OR location_city = 'LAGUNA HILLS' OR location_city = 'LAGUNA NIGUEL' OR
            location_city = 'LAGUNA WOODS' OR location_city = 'LAKE FOREST' OR location_city = 'LAS FLORES' OR
            location_city = 'LONG BEAC' OR location_city = 'LONG BEACH' OR location_city = 'LOS ALAMITOS' OR
            location_city = 'LOS ALMAITOS' OR location_city = 'LOS ANGELES' OR location_city = 'LYNWOOD' OR
            location_city = 'MIDWAY CITY' OR location_city = 'MISSION VALLEY' OR location_city = 'MISSION VIEJO' OR
            location_city = 'MODJESKA' OR location_city = 'MONARCH BEACH' OR location_city = 'NEW PORT BEACH' OR
            location_city = 'NEWPORT' OR location_city = 'NEWPORT BEACH' OR location_city = 'NEWPORT COAST' OR
            location_city = 'NORCO' OR location_city = 'OLINDA' OR location_city = 'ONTARIO' OR
            location_city = 'ORANGE' OR
            location_city = 'PEAK' OR location_city = 'PLACENTIA' OR location_city = 'PLACNETIA' OR
            location_city = 'RANCHO NIGUEL' OR location_city = 'RANCHO SANTA MARGARI' OR location_city = 'RIVERSIDE' OR
            location_city = 'ROSEMEAD' OR location_city = 'S W CORONA' OR location_city = 'SAN CLEMENTE' OR
            location_city = 'SAN JUAN' OR location_city = 'SAN JUAN CAPISTRANO' OR location_city = 'SAN JUAN CAPISTRANSO' OR
            location_city = 'SAN JUAN CAPITRANO' OR location_city = 'SANTA  ANA' OR location_city = 'SANTA ANA' OR
            location_city = 'SANTA ANNA' OR location_city = 'SANTA AVE' OR location_city = 'SANTIAGO PEAK' OR
            location_city = 'SEAL BEACH' OR location_city = 'SIERRA PEAK' OR location_city = 'SIGNAL HILL' OR
            location_city = 'SILVERADO' OR location_city = 'SILVERADO CANYON' OR location_city = 'SIVERADO' OR
            location_city = 'SO. COAST METRO' OR location_city = 'SOUTH LAGUNA' OR location_city = 'SOUTHWEST CORONA' OR
            location_city = 'STANTON' OR location_city = 'SUNSET BEACH' OR location_city = 'SW CORONA' OR
            location_city = 'TRABUCO CANYON' OR location_city = 'TUSTIN' OR location_city = 'VIEJO' OR
            location_city = 'VILLA PARK' OR location_city = 'WESTMINISTER' OR location_city = 'WHILLIER' OR
            location_city = 'WHITTIER' OR location_city = 'WILMINTON' OR location_city = 'YORBA LINDA' OR
            location_city = 'YORBA LINGA'
          )
        ) OR
        location_county = 'PLACER' OR
        location_county = 'PLUMAS' OR
        location_county = 'RIVERSIDE' OR
        location_county = 'SACRAMENTO' OR
        location_county = 'SAN BENITO' OR
        location_county = 'SAN BERNARDINO' OR
        location_county = 'SAN DIEGO' OR
        location_county = 'SAN FRANCISCO' OR
        location_county = 'SAN JOAQUIN' OR
        location_county = 'SAN LUIS OBISPO' OR
        location_county = 'SAN MATEO' OR
        location_county = 'SANTA BARBARA' OR
        location_county = 'SANTA CLARA' OR
        location_county = 'SANTA CRUZ' OR
        location_county = 'SHASTA' OR
        location_county = 'SIERRA' OR
        location_county = 'SISKIYOU' OR
        location_county = 'SOLANO' OR
        location_county = 'SONOMA' OR
        location_county = 'STANISLAUS' OR
        location_county = 'SUTTER' OR
        location_county = 'TEHAMA' OR
        location_county = 'TRINITY' OR
        location_county = 'TULARE' OR
        location_county = 'TUOLUMNE' OR
        location_county = 'VENTURA' OR
        location_county = 'YOLO' OR
        location_county = 'YUBA'
    "
  )


  return()

}


# Pre-Join Queries (CA)
# --------------------------------------------------
f.dbmsprejoinqueriesCA <- function( s4.db ) {


  # deleting non-CA records by location_state
  print( paste0( "deleting non-'CA' records by location_state", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "DELETE FROM PUBACC_LO
      WHERE NOT ( location_state = 'CA' OR location_state = '' )
    "
  )


  # deleting non-CA records by state_code
  print( paste0( "deleting non-'CA' records by state_code", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "DELETE FROM PUBACC_CP
      WHERE NOT ( state_code = 'CA' )
    "
  )


  return()

}


# Pre-Join Queries
# --------------------------------------------------
f.dbmsprejoinqueries <- function( s4.db ) {


  # deleting cellular frequencies
  print( paste0( "deleting cellular frequencies", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "DELETE FROM PUBACC_FR
      WHERE ( frequency_assigned + 0 >= 825 AND frequency_assigned + 0 <= 849 )
        OR
            ( frequency_assigned + 0 >= 869 AND frequency_assigned + 0 <= 894 )
    "
  )


  # deleting frequencies above 1.3 GHz
  print( paste0( "deleting frequencies above 1.3 GHz", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "DELETE FROM PUBACC_FR
      WHERE ( frequency_assigned + 0 ) > 1300
    "
  )


  # changing lower case to upper case for 'CA' records
  print( paste0( "changing lower case to upper case for 'CA'", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "UPDATE PUBACC_CP
      SET control_address = UPPER( control_address ),
        control_city = UPPER( control_city ),
        control_county = UPPER( control_county ),
        state_code = UPPER( state_code )
    "
  )


  # changing lower case to upper case for the 'entity_name' field
  print( paste0( "changing lower case to upper case for the 'entity_name' field", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "UPDATE PUBACC_EN
      SET entity_name = UPPER( entity_name )
    "
  )


  # changing lower case to upper case for the various 'location_' fields
  print( paste0( "changing lower case to upper case for the various 'location_' fields", "..." ))
  s.rows <- dbExecute( conn = s4.db,
    "UPDATE PUBACC_LO
      SET location_address = UPPER( location_address ),
        location_city = UPPER( location_city ),
        location_county = UPPER( location_county ),
        location_state = UPPER( location_state )

    "
  )


  return()

}


# Import a single Data File
# --------------------------------------------------
f.dbmsimportafile <- function( s.zip.file.prefix, s.data.file.prefix, s4.db ) {

  # set important constants

  # these are all .dat files
  s.data.file.ext <- ".dat"


  # derive specific variables

  # make a correct file name
  s.data.file.name <- paste0( s.zip.file.prefix, "-", s.data.file.prefix, s.data.file.ext )

  # make a correct Database table name
  s.table.name <- paste0( "PUBACC_", s.data.file.prefix )


#LOAD DATA LOCAL INFILE 'l_coast-CP.dat' INTO TABLE PUBACC_CP FIELDS TERMINATED BY '\|';


  # document the progress to the user
  print( paste0( "importing ", s.data.file.name, "..." ))


  # import the data
  res <- dbWriteTable( conn = s4.db,
                       name = s.table.name,
                       value = s.data.file.name,
                       row.names = FALSE,
                       append = TRUE,
                       header = FALSE,
                       sep ="|"
  )


#read.csv.sql(file, sql = "select * from file", header = TRUE, sep = ",", 
#row.names, eol, skip, filter, nrows, field.types, 
#colClasses, dbname = tempfile(), drv = "SQLite", ...)

#  read.csv.sql( file = s.data.file.name,
#                sql = 'SELECT * FROM file',
#                header = FALSE,
#                sep = "|",
#                row.names = FALSE,
#                dbname = "uls.sqlite",
#                drv = "SQLite"
#  )


  return()

}


# Import All Data
# --------------------------------------------------
f.dbmsimport <- function( v.zip.file.prefixes, v.data.file.prefixes, s4.db ) {

  # set important constants

  # derive specific variables


  # extract (uncompress) each .zipped file 
  for( s.zip.file.prefix in v.zip.file.prefixes ) {
    for( s.data.file.prefix in v.data.file.prefixes ) {
      f.dbmsimportafile( s.zip.file.prefix, s.data.file.prefix, s4.db )
    }
  }


  return()

}


# Create Database tables
# --------------------------------------------------
f.dbmstables <- function( s4.db ) {

  # create the tables in the DB

  # document the progress to the user
  print( paste0( "creating the tables in the database", "..." ))


  # CP

  if( dbExistsTable( conn = s4.db, name = "PUBACC_CP" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_CP" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_CP (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      control_point_action_performed char(1),
      control_point_number      integer,
      control_address           varchar(80),
      control_city              char(20),
      state_code                char(2),
      control_phone             char(10),
      control_county            varchar(60),
      status_code               char(1),
      status_date               char(10),
      db_id                     char(9)
    )"
  )


  # EM

  if( dbExistsTable( conn = s4.db, name = "PUBACC_EM" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_EM" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_EM (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      location_number           integer,
      antenna_number            integer,
      frequency_assigned        numeric(16,8),
      emission_action_performed char(1),
      emission_code             char(10),
      digital_mod_rate          numeric(8,1),
      digital_mod_type          char(7),
      frequency_number          integer,
      status_code               char(1),
      status_date               char(10),
      emission_sequence_id      integer,
      db_id                     char(9)
    )"
  )


  # EN

  if( dbExistsTable( conn = s4.db, name = "PUBACC_EN" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_EN" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_EN (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      entity_type               char(2),
      licensee_id               char(9),
      entity_name               varchar(200),
      first_name                varchar(20),
      mi                        char(1),
      last_name                 varchar(20),
      suffix                    char(3),
      phone                     char(10),
      fax                       char(10),
      email                     varchar(50),
      street_address            varchar(60),
      city                      varchar(20),
      state                     char(2),
      zip_code                  char(9),
      po_box                    varchar(20),
      attention_line            varchar(35),
      sgin                      char(3),
      frn                       char(10),
      applicant_type_code       char(1),
      applicant_type_code_other char(40),
      status_code               char(1),
      status_date               char(10),
      db_id                     char(9)
    )"
  )


  # FR

  if( dbExistsTable( conn = s4.db, name = "PUBACC_FR" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_FR" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_FR (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      frequency_action_performed char(1),
      location_number           integer,
      antenna_number            integer,
      class_station_code        char(4),
      op_altitude_code          char(2),
      frequency_assigned        numeric(16,8),
      frequency_upper_band      numeric(16,8),
      frequency_carrier         numeric(16,8),
      time_begin_operations     integer,
      time_end_operations       integer,
      power_output              numeric(15,3),
      power_erp                 numeric(15,3),
      tolerance                 numeric(6,5),
      frequency_ind             char(1),
      status                    char(1),
      eirp                      numeric(7,1),
      transmitter_make          varchar(25),
      transmitter_model         varchar(25),
      auto_transmitter_power_control char(1),
      cnt_mobile_units          integer,
      cnt_mob_pagers            integer,
      freq_seq_id               integer,
      status_code               char(1),
      status_date               char(10),
      date_first_used           char(10),
      db_id                     char(9)
    )"
  )


  # HD

  if( dbExistsTable( conn = s4.db, name = "PUBACC_HD" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_HD" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_HD (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      license_status            char(1),
      radio_service_code        char(2),
      grant_date                char(10),
      expired_date              char(10),
      cancellation_date         char(10),
      eligibility_rule_num      char(10),
      applicant_type_code       char(1),
      alien                     char(1),
      alien_government          char(1),
      alien_corporation         char(1),
      alien_officer             char(1),
      alien_control             char(1),
      revoked                   char(1),
      convicted                 char(1),
      adjudged                  char(1),
      involved_reserved      	char(1),
      common_carrier            char(1),
      non_common_carrier        char(1),
      private_comm              char(1),
      fixed                     char(1),
      mobile                    char(1),
      radiolocation             char(1),
      satellite                 char(1),
      developmental_or_sta      char(1),
      interconnected_service    char(1),
      certifier_first_name      varchar(20),
      certifier_mi              char(1),
      certifier_last_name       varchar(20),
      certifier_suffix          char(3),
      certifier_title           char(40),
      gender                    char(1),
      african_american          char(1),
      native_american           char(1),
      hawaiian                  char(1),
      asian                     char(1),
      white                     char(1),
      ethnicity                 char(1),
      effective_date            char(10),
      last_action_date          char(10),
      auction_id                char(1),
      reg_stat_broad_serv       char(1),
      band_manager              char(1),
      type_serv_broad_serv      char(1),
      alien_ruling              char(1),
      licensee_name_change	    char(1),
      whitespace_ind            char(1),
      db_id                     char(9)
    )"
  )


  # HS

  if( dbExistsTable( conn = s4.db, name = "PUBACC_HS" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_HS" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_HS (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      call_sign                 char(10),
      date                      char(10),
      code                      char(6),
      db_id                     char(9)
    )"
  )


  # LO

  if( dbExistsTable( conn = s4.db, name = "PUBACC_LO" )) {
    dbRemoveTable( conn = s4.db, name = "PUBACC_LO" )
  }

  s.rows <- dbExecute( conn = s4.db,
    "CREATE TABLE PUBACC_LO (
      record_type               char(2),
      unique_system_identifier  numeric(9,0),
      uls_file_number           char(14),
      ebf_number                varchar(30),
      call_sign                 char(10),
      location_action_performed char(1),
      location_type_code        char(1),
      location_class_code       char(1),
      location_number           integer,
      site_status               char(1),
      corresponding_fixed_location integer,
      location_address          varchar(80),
      location_city             char(20),
      location_county           varchar(60),
      location_state            char(2),
      radius_of_operation       numeric(5,1),
      area_of_operation_code    char(1),
      clearance_indicator       char(1),
      ground_elevation          numeric(7,1),
      lat_degrees               integer,
      lat_minutes               integer,
      lat_seconds               numeric(3,1),
      lat_direction             char(1),
      long_degrees              integer,
      long_minutes              integer,
      long_seconds              numeric(3,1),
      long_direction            char(1),
      max_lat_degrees           integer,
      max_lat_minutes           integer,
      max_lat_seconds           numeric(3,1),
      max_lat_direction         char(1),
      max_long_degrees          integer,
      max_long_minutes          integer,
      max_long_seconds          numeric(3,1),
      max_long_direction        char(1),
      nepa                      char(1),
      quiet_zone_notification_date char(10),
      tower_registration_number char(10),
      height_of_support_structure numeric(7,1),
      overall_height_of_structure numeric(7,1),
      structure_type            char(6),
      airport_id                char(4),
      location_name             char(20),
      units_hand_held           integer,
      units_mobile              integer,
      units_temp_fixed          integer,
      units_aircraft            integer,
      units_itinerant           integer,
      status_date               char(10),
      status_code               char(1),
      earth_station_agreement   char(1),
      db_id                     char(9)
    )"
  )


  return()

}


# Create DB
# --------------------------------------------------
f.dbmscreate <- function( s.dbname ) {


  # document the progress to the user
  print( paste0( "creating the database", "..." ))

  # delete any prior Database file (if exists)
  # old way (works)
#  if( file.exists( s.dbname )) {
#    file.remove( s.dbname )
#  }

  # delete any prior Database file (if exists)
  # new way (also works)
  unlink( x = s.dbname )

  # open an SQLite Database connection handle via RSQLite
  # (create it--in memory or on disk--if it doesn't exist)
  s4.db <- dbConnect( drv = SQLite(), dbname = s.dbname )


  return( s4.db )

}


# Create Database, Import Data, and Fix Minor Anomalies
# --------------------------------------------------
f.dbms <- function( v.zip.file.prefixes, v.data.file.prefixes, s.dbname ) {

  # set important constants

  # derive specific variables


  # create a Database
  s4.db <- f.dbmscreate( s.dbname )


  # create tables in the Database
  f.dbmstables( s4.db )


  # import all data into the Database
  f.dbmsimport( v.zip.file.prefixes, v.data.file.prefixes, s4.db )


  # imputation queries
#  f.dbmsimputationqueries( s4.db )


  # pre-join queries
  f.dbmsprejoinqueries( s4.db )


  # create indexes in the Database
  f.dbmsindexes( s4.db )


  # join query (US)
#  f.dbmsjoinqueryUS( s4.db )


  # post-join queries (US)
#  f.dbmspostjoinqueriesUS( s4.db )


  # pre-join queries (CA)
  f.dbmsprejoinqueriesCA( s4.db )


  # join query (CA)
  f.dbmsjoinqueryCA( s4.db )


  # post-join queries (CA)
  f.dbmspostjoinqueriesCA( s4.db )


  # document the progress to the user
  print( paste0( "disconnecting the database", "..." ))

  # close the Database connection handle
  dbDisconnect( conn = s4.db )


  return()

}


# Fix a single .dat file (new method...that is, it *doesn't* depend upon PERL)
# --------------------------------------------------
f.fixAFile <- function( s.zip.file.prefix, s.data.file.prefix ) {

  # set important constants
  s.data.file.new <- "-new"

  # these are all .dat files
  s.data.file.ext <- ".dat"


  # make the correct filename
  s.data.file <- paste0( s.zip.file.prefix, "-", s.data.file.prefix )

  s.data.file.name <- paste0( s.data.file, s.data.file.ext )

  s.data.file.name.new <- paste0( s.data.file, s.data.file.new, s.data.file.ext )


  # document the progress to the user
  print( paste0( "preprocessing ", s.data.file, "..." ))
#  message( paste0( "preprocessing ", s.data.file, "..." ))


  # delete the (-new) file (it may not exist)
  if( file.exists( s.data.file.name.new )) {
    file.remove( s.data.file.name.new )
  }


  # read the text file (line-by-line)
  # (there appears to be no other way to do this...sigh)
#  df.data <- read.table( file = s.data.file.name, header = FALSE, sep = "|", colClasses = "character" )
#  v.data <- scan( file = s.data.file.name, what = "raw", sep = "|" )

  con <- file( description = s.data.file.name, open = "r" )
  v.lines <- readLines( con = con )
  close( con )


  # use only valid lines (the FCC ULS database embeds CR/LFs in text files...argh!)

  # get the number of field separators ("|") from the first row
  # (we assume that the first row is *correct*...grin)
  # (note also the double back-slash to specify the separator)
  s.lines.sep.n <- str_count( v.lines[ 1 ], "\\|" )


  # only use lines with the correct number of field separators
  # (we assume that the first row is *correct*...grin)
  # and add the file source too
print( paste0( "original length = ", format( length( v.lines ), big.mark = ",", scientific = FALSE, trim = TRUE ), " lines..." ))
#format(c(123,1234),big.mark=",", trim=TRUE)
  # this is the vectorized approach (relatively fast)
  # keep only the lines that are correct
#  v.lines <- v.lines[ which( str_count( v.lines, "\\|" ) == s.lines.sep.n ) ]
#  v.lines <- subset( v.lines, subset = str_count( v.lines, "\\|" ) == s.lines.sep.n )
  v.lines <- v.lines[ str_count( v.lines, "\\|" ) == s.lines.sep.n ]

  # add the DB_ID field (it's just the zip.file.prefix)
  v.lines <- paste0( v.lines, "|", s.zip.file.prefix )
#  v.lines <- paste0( v.lines, "|", "" )

# this is the non-vectorized approach (very slow)
#  v.lines <- vector( mode = "character", length = 0 )
#  v.lines <- v.lines[ 1 ]
#  for( s.counter in 2: length( v.lines )) {
#    if( str_count( v.lines[ s.counter ], "\\|" ) == s.lines.sep.n ) {
#      v.lines.new <- c( v.lines.new, v.lines[ s.counter ] )
#    }
#  }
print( paste0( "'fixed CR/LFs' length = ", format( length( v.lines ), big.mark = ",", scientific = FALSE, trim = TRUE ), " lines..." ))


  # replace any double quotes with a space
  # (or else the line won't import correctly into SQLite)
  v.lines <- gsub( pattern = '\"', replacement = " ", x = v.lines, fixed = TRUE )


  # write the text file (line-by-line)
  # we have to do it this way because we *read* the file line-by-line
  con <- file( description = s.data.file.name.new, open = "w" )
  writeLines( text = v.lines, con = con )
  close( con )


  # delete the original .dat file (it should exist)
  if( file.exists( s.data.file.name )) {
    file.remove( s.data.file.name )
  }


  # rename the new .dat file to the original .dat file
  file.rename( from = s.data.file.name.new, to = s.data.file.name )


  # release the memory for v.lines (it may be a lot of memory)
  rm( v.lines )


  return()

}


# Fix a single .dat file (uses PERL now, but we'll change that--or port it--later)
# --------------------------------------------------
f.fixAFilePERL <- function( s.zip.file.prefix, s.data.file.prefix, s.perl.app ) {

  # set important constants
  s.data.file.new <- "-new"

  # these are all .dat files
  s.data.file.ext <- ".dat"

  # this is the PERL interpreter
  s.perl <- "perl -w" 


  # make the correct filename
  s.data.file <- paste0( s.zip.file.prefix, "-", s.data.file.prefix )

  s.data.file.name <- paste0( s.data.file, s.data.file.ext )


  s.data.file.name.new <- paste0( s.data.file, s.data.file.new, s.data.file.ext )

  s.perl.string <- paste( s.perl, s.perl.app, s.data.file, sep = " " )


  # document the progress to the user
  print( paste0( "preprocessing ", "(", s.perl.app, ") ", s.data.file, "..." ))


#: Ensure this file doesn't exist
#del %fileDat%-new.dat
#
#: transform the file appropriately
#perl -w preprocess-DeleteBlankLines.pl %fileDat%
#del %fileDat%.dat
#ren %fileDat%-new.dat %fileDat%.dat

  # delete a possible existing (-new) file
  if( file.exists( s.data.file.name.new )) {
    file.remove( s.data.file.name.new )
  }

#print( s.perl )
#print( s.perl.app )
#print( s.perl.string )

  # fix the .dat file with a PERL script as needed
  l.cap <- system( s.perl.string, intern = TRUE )
  print( l.cap )

  # delete the original .dat file
  if( file.exists( s.data.file.name )) {
    file.remove( s.data.file.name )
  }

  # rename the new .dat file to the original .dat file
  file.rename( from = s.data.file.name.new, to = s.data.file.name )


  return()

}


# Preprocess a .dat file (prior to DBMS import)
# --------------------------------------------------
f.preprocessAFile <- function( s.zip.file.prefix, s.data.file.prefix ) {

  # fix each file
  f.fixAFile( s.zip.file.prefix, s.data.file.prefix )


  # delete blank lines (old)

  # use the specific (custom) PERL application that we need
  # delete blank lines
  s.perl.app <- "preprocess-DeleteBlankLines.pl"

#  f.fixAFilePERL( s.zip.file.prefix, s.data.file.prefix, s.perl.app )


  # use the specific (custom) PERL application that we need
  # fix embedded CR/LFs
  s.perl.app <- "preprocess-FixEmbeddedCRLFs.pl"

#  f.fixAFilePERL( s.zip.file.prefix, s.data.file.prefix, s.perl.app )


  # use the specific (custom) PERL application that we need
  # add DB_ID field
  s.perl.app <- "preprocess-AddDB_IDField.pl"

#  f.fixAFilePERL( s.zip.file.prefix, s.data.file.prefix, s.perl.app )


  return()

}


# Rename a file
# --------------------------------------------------
f.renameAFile <- function( s.zip.file.prefix, s.data.file.name ) {

  # document the progress to the user
  print( paste0( "renaming ", s.zip.file.prefix, "-", s.data.file.name, "..." ))

  # rename the file (because the 'counts' and .dat files aren't unique across .zipped files...especially in a single directory)
  file.rename( from = s.data.file.name, to = paste0( s.zip.file.prefix, "-", s.data.file.name ))


  return()

}


# Extract a File
# --------------------------------------------------
f.extractAFile <- function( s.zip.file.name, s.file.name ) {

  # document the progress to the user
  print( paste0( "extracting ", s.file.name, " from ", s.zip.file.name, "..." ))

  # this function is in Base R
  unzip( s.zip.file.name, files = s.file.name )


  return()

}


# Extract All 'counts' and '.dat' files
# --------------------------------------------------
f.extractAllFiles <- function( s.zip.file.prefix, v.data.file.prefixes ) {

  # set important constants

  s.zip.file.ext <- ".zip"
  s.data.file.ext <- ".dat"

  # make a correct file name
  s.zip.file.name <- paste0( s.zip.file.prefix, s.zip.file.ext )


  # we need the special 'counts' file (e.g., to double-check the correct number of records imported, etc.)
  s.counts.file.name <- "counts"


  # extract the 'counts' file
  f.extractAFile( s.zip.file.name, s.counts.file.name )

  # rename the 'counts' file
  f.renameAFile( s.zip.file.prefix, s.counts.file.name )


  # extract, rename, and clean up each .dat file

  for( s.data.file.prefix in v.data.file.prefixes ) {

  # extract each .dat file
    s.data.file.name <- paste0( s.data.file.prefix, s.data.file.ext )
    l.data <- f.extractAFile( s.zip.file.name, s.data.file.name )

  # rename each .dat file
    l.data <- f.renameAFile( s.zip.file.prefix, s.data.file.name )

  # clean up each .dat file prior to DBMS import
    l.data <- f.preprocessAFile( s.zip.file.prefix, s.data.file.prefix )

  }


  return()

}


# Download a .zipped file
# --------------------------------------------------
f.downloadAFile <- function( s.zip.file.prefix ) {

  # set important constants

  # this is the FCC ULS download area
  s.url.basepath <- "http://wireless.fcc.gov/uls/data/complete/"

  # these are all .zipped (compressed) files
  s.zip.file.ext <- ".zip"


  # derive specific variables

  # make a correct file name
  s.zip.file.name <- paste0( s.zip.file.prefix, s.zip.file.ext )

  # make a correct URL
  s.url <- paste0( s.url.basepath, s.zip.file.name )


  # delete the file (it may not exist)
  if( file.exists( s.zip.file.name )) {
    file.remove( s.zip.file.name )
  }

  # document the progress to the user
  print( paste0( "downloading ", s.zip.file.name, "..." ))

  # download a single .zipped file
  # (downloader::download is simply a wrapper for the 'download.file' function in Base R)

  # this is needed for home (non-VPN) use
#  options( download.file.method = "wininet" )

  # this works--as is--from a *.csun.edu address (including over the VPN)
  download( url = s.url, destfile = s.zip.file.name )


  return()

}


# Get All .zipped files
# --------------------------------------------------
f.getAllFiles <- function( v.zip.file.prefixes, v.data.file.prefixes ) {

  # set important constants

  # FCC ULS file prefix ("l" = "license")
#  s.zip.file.prefix <- "l_"


  # download each .zipped file
  time.tic <- Sys.time()
#  l.zip <- lapply( v.zip.file.name, f.downloadAFile )
  for( s.zip.file.prefix in v.zip.file.prefixes ) {

  # download each .zipped file (old)
    l.zip <- f.downloadAFile( s.zip.file.prefix )

  # extract (uncompress) each .zipped file 
    l.zip <- f.extractAllFiles( s.zip.file.prefix, v.data.file.prefixes )
  }

  print( Sys.time() - time.tic )


  return()

}


# Main
# --------------------------------------------------
f.main <- function() {

  # turn warnings into errors (use this for debugging)
  options( "warn" = 2 )
  # list warnings as they occur (use this for debugging)
  options( "warn" = 1 )

  #  print a blank line (because of the output generated by loading the required libraries)
  cat( "\n" )


  # double-check the default directory...
  # setwd( "c:\\uls\\new" )
  # setwd( "/home/wsmith/uls" )


  # these are the specific *.zipped* files that we want to download from the FCC ULS web site
  # the prefix "l" is for "licenses", as opposed to "a" for "applications"
  # Land Mobile - Private
  # Land Mobile - Commercial
  # Land Mobile - Broadcast Auxillary
  # Maritime Coast & Aviation Ground
  # Microwave
  # Paging
  v.zip.file.prefixes <- c( "l_LMpriv", "l_LMcomm", "l_LMbcast", "l_coast", "l_micro", "l_paging" )

  # within each .zip file, these are the specific *.dat* files that we want
  # Control Point
  # Emission
  # Entity
  # Frequency
  # Application/License Header
  # History
  # Location
  v.data.file.prefixes <- c( "CP", "EM", "EN", "FR", "HD", "HS", "LO" )


  # name of Database in SQLite
  # use if you have >=32GB of memory
  # (the full US Database--as expressed in a SQLite3 Database--is ~ 24GB)
  s.dbname <- ":memory:"
  # use if you have <32GB of memory
#  s.dbname <- "uls.sqlite"


  # Step 1
  # download, extract (unzip), rename, and preprocess (e.g., fix two-line records) all .zip and .dat files
  # http://wireless.fcc.gov/uls/index.htm?job=transaction
  # total elapsed time ~ 21 minutes (the biggest factor is the throttled download speed from the FCC web site)
#  f.getAllFiles( v.zip.file.prefixes, v.data.file.prefixes )


  # Step 2
  # create the Database, import the data, fix minor anomalies, do INNER JOINS, and write output files
  # total elapsed time ~ 54 minutes (the biggest factor is the 'Emissions' join)
  f.dbms( v.zip.file.prefixes, v.data.file.prefixes, s.dbname )


  # reset warnings back to the default (which is 0) (use this for debugging)
  options( "warn" = 0 )

  return()

}


# call the main routine
f.main()

