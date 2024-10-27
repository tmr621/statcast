rm(list=ls()) 


library(baseballr)
library(tidyverse)
library(sqldf)
library(RPostgres)
library(DBI)



zombies <- annual_statcast_query(2021)                         
titties <- sqldf('SELECT * FROM HoldingCell')
crashes_join_roads <- sqldf('SELECT * FROM zombies WHERE release_speed < 70')
crashes_join_roads <- sqldf('SELECT * FROM zombies WHERE game_date = "2021-06-07"')



pw<- {
  ""
}

con <- dbConnect(RPostgres::Postgres()
                 , host='localhost'
                 , port='5432'
                 , dbname='pitchfx'
                 , user='postgres'
                 , password=pw)


rm(pw) # removes the password



#######################

annual_statcast_query <- function(season) {
  
  dates <- seq.Date(as.Date(paste0(season, '-03-01')),
                    as.Date(paste0(season, '-12-01')), by = 'week')
  
  date_grid <- tibble(start_date = dates, 
                      end_date = dates + 6)
  
  safe_savant <- safely(scrape_statcast_savant)
  
  payload <- map(.x = seq_along(date_grid$start_date), 
                 ~{message(paste0('\nScraping week of ', date_grid$start_date[.x], '...\n'))
                   
                   payload <- safe_savant(start_date = date_grid$start_date[.x], 
                                          end_date = date_grid$end_date[.x], type = 'pitcher')
                   
                   return(payload)
                 })
  
  payload_df <- map(payload, 'result')
  
  number_rows <- map_df(.x = seq_along(payload_df), 
                        ~{number_rows <- tibble(week = .x, 
                                                number_rows = length(payload_df[[.x]]$game_date))}) %>%
    filter(number_rows > 0) %>%
    pull(week)
  
  payload_df_reduced <- payload_df[number_rows]
  
  combined <- payload_df_reduced %>%
    bind_rows()
  
  return(combined)
  
}



###########################

format_append_statcast <- function(df) {
  
  # function for appending new variables to the data set
  
  additional_info <- function(df) {
    
    # apply additional coding for custom variables
    
    df$hit_type <- with(df, ifelse(type == "X" & events == "single", 1,
                                   ifelse(type == "X" & events == "double", 2,
                                          ifelse(type == "X" & events == "triple", 3, 
                                                 ifelse(type == "X" & events == "home_run", 4, NA)))))
    
    df$hit <- with(df, ifelse(type == "X" & events == "single", 1,
                              ifelse(type == "X" & events == "double", 1,
                                     ifelse(type == "X" & events == "triple", 1, 
                                            ifelse(type == "X" & events == "home_run", 1, NA)))))
    
    df$fielding_team <- with(df, ifelse(inning_topbot == "Bot", away_team, home_team))
    
    df$batting_team <- with(df, ifelse(inning_topbot == "Bot", home_team, away_team))
    
    df <- df %>%
      mutate(barrel = ifelse(launch_angle <= 50 & launch_speed >= 98 & launch_speed * 1.5 - launch_angle >= 117 & launch_speed + launch_angle >= 124, 1, 0))
    
    df <- df %>%
      mutate(spray_angle = round(
        (atan(
          (hc_x-125.42)/(198.27-hc_y)
        )*180/pi*.75)
        ,1)
      )
    
    df <- df %>%
      filter(!is.na(game_year))
    
    return(df)
  }
  
  df <- df %>%
    additional_info()
  
  df$game_date <- as.character(df$game_date)
  
  df <- df %>%
    arrange(game_date)
  
  df <- df %>%
    filter(!is.na(game_date))
  
  df <- df %>%
    ungroup()
  
  df <- df %>%
    select(setdiff(names(.), c("error")))
  
  cols_to_transform <- c("fielder_2", "pitcher_1", "fielder_2_1", "fielder_3",
                         "fielder_4", "fielder_5", "fielder_6", "fielder_7",
                         "fielder_8", "fielder_9")
  
  df <- df %>%
    mutate_at(.vars = cols_to_transform, as.numeric) %>%
    mutate_at(.vars = cols_to_transform, function(x) {
      ifelse(is.na(x), 999999999, x)
    })
  
  data_base_column_types <- read_csv("https://app.box.com/shared/static/q326nuker938n2nduy81au67s2pf9a3j.csv")
  
  character_columns <- data_base_column_types %>%
    filter(class == "character") %>%
    pull(variable)
  
  numeric_columns <- data_base_column_types %>%
    filter(class == "numeric") %>%
    pull(variable)
  
  integer_columns <- data_base_column_types %>%
    filter(class == "integer") %>%
    pull(variable)
  
  df <- df %>%
    mutate_if(names(df) %in% character_columns, as.character) %>%
    mutate_if(names(df) %in% numeric_columns, as.numeric) %>%
    mutate_if(names(df) %in% integer_columns, as.integer)
  
  return(df)
}

#######################



delete_and_upload <- function(df, 
                              year, 
                              db_driver = "PostgreSQL", 
                              dbname, 
                              user, 
                              password, 
                              host = 'local_host', 
                              port = 5432) {
  
  pg <- dbDriver(db_driver)
  
  statcast_db <- dbConnect(pg, 
                           dbname = dbname, 
                           user = user, 
                           password = password,
                           host = host, 
                           port = posrt)
  
  query <- paste0('DELETE from statcast where game_year = ', year)
  
  dbGetQuery(statcast_db, query)
  
  dbWriteTable(statcast_db, "statcast", df, append = TRUE)
  
  dbDisconnect(statcast_db)
  rm(statcast_db)
}

#################################



# create table and upload first year

payload_statcast <- zombies #annual_statcast_query(2018)

df <- zombies ## format_append_statcast(df = payload_statcast)

# connect to your database
# here I am using my personal package that has a wrapper function for this

pw<- {
  "Chaos22!"
}

statcast_db <- dbConnect(RPostgres::Postgres()
                 , host='localhost'
                 , port='5432'
                 , dbname='pitchfx'
                 , user='postgres'
                 , password=pw)

rm(pw) # removes the password

df <- df[,c(1,3,4,5)] 
df <- df[c(1:10),]

df <- data.frame(df)

dbWriteTable(statcast_db, name = Id(schema = "StatCast", table = 'Movement'), df,)
             


dbWriteTable(statcast_db, name = Id(schema = "StatCast", table = 'StatCast'), df, field.types = NULL, row.names = FALSE, overwrite = TRUE, allow.keywords = FALSE)
             

# disconnect from database

myDBconnections::disconnect_Statcast_postgreSQL(statcast_db)

# or you can simply run 
# DBI::dbDisconnect(statcast_db)

rm(df)
gc()
