# This script uploads logger read outs to the VIU CHRL mysql database on island
# hosting. The basic workflow is retrieve the last entry of data from the db and
# then filter the logger file to after what already exists on the db then create
# a continuous sequence of datetimes in case we are missing some data
# transmissions then we upload the continuous sequence of data to the db

#### define some constants ####

library(DBI)
library(tidyverse)

rm(list=ls()) # clear environment

db_tbl_name <- 'raw_steph1' # this is the table we are changing on island hosting
new_logger_data_path <- 'data/RC_db_S2_CSci_upd - Copy.txt' # this is the path to the file that has the new logger data

# define credentials to query db, note that your IP must be whitelisted for this to work
source('config.r')

#### pull in new logger data ####

raw_data <- read.csv(new_logger_data_path, skip = 1, na.strings = 'NaN')

raw_data$DateTime <- paste0(raw_data$year,'-',
                            raw_data$month, '-',
                            raw_data$day, ' ',
                            raw_data$hour, ':',
                            raw_data$minute, ':',
                            raw_data$second) |> as.POSIXct(tz = 'UTC')

clean_data <- raw_data |> select(DateTime, WatYr:Solar_Rad)

#### retrieve existing data from db ####

# grab the last hour of data from the table to see what else we need to add to it
query <- paste0("SELECT * FROM ", db_tbl_name, " WHERE DateTime=(select max(DateTime) from ", db_tbl_name,")")

conn <- do.call(DBI::dbConnect, args)

db_data <- DBI::dbGetQuery(conn, query)

DBI::dbDisconnect(conn)

# only if we get data above do we need to apply this filtering
if(exists("db_data")){
  last_datetime <- tail(db_data$DateTime, n = 1)

  # select everything after what we already have on the database
  clean_data <- clean_data |>
    filter(DateTime > last_datetime)

} else {
  paste('There was no data on the table', db_tbl_name, 'on island hosting. Continuing on with the entire logger file.')
}

# get the range of data so we can create a continuous sequence
date_range <- range(clean_data$DateTime)

date_seq <- as.POSIXct(seq(date_range[1], date_range[2], by = 60 * 60), tz = 'UTC')

# join on full sequence
date_seq_df <- data.frame(DateTime = date_seq)

clean_data <- date_seq_df |>
  left_join(clean_data, by = 'DateTime')

#### output new data to db ####

# create connection to db
conn <- do.call(DBI::dbConnect, args)

# make sure to not leave connection open when we leave
on.exit(DBI::dbDisconnect(conn))

DBI::dbAppendTable(conn = conn, name = 'raw_steph2', value = clean_data)

DBI::dbDisconnect(conn)
