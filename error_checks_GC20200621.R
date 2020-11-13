#DATA CHECKING AND CLEANING
#this code creates a number of graphs, tables, and dataframe columns (within R,
#does not create any external files) to identify data entry errors.

#Anywhere there is a capitalized CHECK written in the annotation, check the
#relevant dataframe or graph for errors as directed.

#Correct any errors found in the access database file.

#After correcting errors in access, I suggest clearing workspace and re-running
#the code before moving to the next step. This will get the corrected data, so
#you don't see errors you have already fixed.

#This code will be easiest to use in RStudio, because many data checks rely on
#being able to sort or filter columns in dataframes, and RStudio makes it Very
#easy to do this.

#inputs: files imported from queries or tables in database file
#        SAMPLE_SPECIMEN_COMBINED (fish data)
#        FISH_T_SAMPLE (site data)
#        FISH_T_LF_STATION_REFERENCE (GCMRC reference site/station data)
#
#        "water_quality_2017.csv"  turbidity and temp data, table with 3
#               columns: START_DATE, turbidity, and water_temp

#        "GC20170402_all_PIT_tags.csv" scanner download data for PIT tags
#               columns: date, time, PITTAG, boat

#outputs: none

#notes: MUST use 32-bit R, not 64-bit R to pull data directly from database
#
#Open the database file before running code to connect R to database.  Access
#doesn't like to open a file that R is already connected to

#setwd("\\\\flag-server/Office/Grand Canyon Downstream/Databases/2019")
#setwd("C:/Users/jboyer.AGFD_PHX/Documents")

require(RODBC) #database interface
require(tidyverse)
require(chron) #for times (base R datetimes include date, can't do time only)

###################### LOAD DATA
#connect to database
#you MUST be running 32 bit R, not 64 bit R, for this to work
#in Rstudio Tools/Global Options/General, then click change button by R version
#database file should be closed - does not like connecting when access is open
#is ok to reopen access once you have connected and fetched needed tables
db <- odbcConnectAccess2007("./data/GC20200621.accdb", case = "nochange")
odbcGetInfo(db) #info about database
sqlTables(db) #see what tables are in database

#load fish data
fish <- sqlFetch(db, #database
                 "SAMPLE_SPECIMEN_COMBINED", #table or query in database
                 na.strings = c(""," ","NA")) #make blanks and spaces = NA

#load sample site data
site <- sqlFetch(db, "FISH_T_SAMPLE", na.strings = c(""," ","NA"))

odbcClose(db)

#load turbidity and temperature data
turbidity <- read.csv("./data/GC20200621_turbidity_temperature.csv")

#load site/station reference table from database
#setwd("//flag-server/Office/Grand Canyon Downstream/Data Error Checks/2019")
station <- read.csv("./data/GC_All_Sites.csv")

#Load scanner download PIT tag data
#setwd("\\\\flag-server/Office/Grand Canyon Downstream/Scanner Downloads/2019")
scan <- read_csv("./data/GC20200621_all_PIT_tags.csv")

################## FORMAT DATA

#remove unnecessary columns with no data (only NA, or only 0 and NA)
fish <- fish[, unlist(lapply(fish, function(x) !all(is.na(x))))] #remove NA
site <- site[, unlist(lapply(site, function(x) !all(is.na(x))))]
fish <- fish[, colSums(fish != 0, na.rm = TRUE) > 0] #remove 0
site <- site[, colSums(site != 0, na.rm = TRUE) > 0]

#format START_DATE columns as date (POSIXct) format
site$START_DATE <- as.Date(site$START_DATE, format = "%m/%d/%Y")
site$END_DATE <- as.Date(site$END_DATE, format = "%m/%d/%Y")
turbidity$START_DATE <- as.Date(turbidity$date)
#next line will give warning, is fine, is just telling you that it couldn't
#convert the missing times (NA) to times format
#site$START_TIME <- times(paste0(site$START_TIME, ":00"))

#if datetime is missing, make datetimes
site$START_DATETIME <- as.POSIXct(ifelse(is.na(site$START_DATETIME) == FALSE,
                               as.character(site$START_DATETIME),
                               ifelse(is.na(site$START_DATE) | is.na(site$START_TIME),
                               NA,
                               paste(as.character(site$START_DATE),
                                         as.character(site$START_TIME)))))

#subset station to only downstream sites (remove lees ferry sites)
#$type <- substr(station$STATION_ID, 1,2)
#station <- station[station$type =="S+",]
#station <- station[, unlist(lapply(station, function(x) !all(is.na(x))))]
#format station IDs in station dataframe to match format in site data
#station$start_RM <- as.numeric(gsub("[[:alpha:]]", "", station$UNPADDED_STATION_ID))
#station$siteID <- paste(station$start_RM, station$SIDE, sep = "")


#add end RM to each site
station$start_RM = station$RiverMile_100ths
R <- station[station$RiverSide == "R",] #dataframe with right only
L <- station[station$RiverSide == "L",] #with left only
#error messages about NAs for next two lines are fine, ignore them
RRM_next <- as.numeric(c(as.vector(R$start_RM), "NA")) #vector of next RM
LRM_next <- as.numeric(c(as.vector(L$start_RM), "NA"))
R$end_RM <- RRM_next[2:length(RRM_next)] #add next RM vector as end_RM column
L$end_RM <- LRM_next[2:length(LRM_next)]
station <- rbind(R, L) #bind R, L dataframes to make complete station dataframe
station$site_length <- station$end_RM - station$start_RM
station <- station[, c("SiteID", "start_RM", "end_RM", "RiverSide")]
colnames(station) <- c("DATASHEET_SAMPLE_ID", "start_RM", "end_RM", "SIDE")
rm(R, L)

############################# CHECK SITE DATA
#sort below columns in site dataframe to see high and low values
#these checks could also be done in access if you prefer
#CHECK for data entry errors for any site where values are not plausible
#WATER_TEMP: 6 - 22 is plausible
#TOTAL_HOURS: 13 - 20 is plausible for hoop net
#TOTAL_CATCH: 0 to several hundred is plausible
#START_DATE and END_DATE should match dates of trip

#If you make any corrections to dates or times in the database, make sure you
#also press the "Merge Date and Time Values" and "Populate Netting Total Hours"
#buttons in the "Edit and View Data" tab to update fields calculated from
#corrected dates and times.

#CHECK for duplicate site, station names
#if any site returns n > 1, check for data entry errors
dup.sites <- site %>%
  group_by(DATASHEET_SAMPLE_ID) %>%
  summarize(n = length(DATASHEET_SAMPLE_ID))

dup.sites <- summarize(group_by(site, DATASHEET_SAMPLE_ID),
                       n = length(DATASHEET_SAMPLE_ID))

#station ID only relevant to hoop nets, does not exist for electrofishing
dup.stations <- site %>%
  group_by(STATION_ID) %>%
  summarize(n = length(STATION_ID))

rm(dup.sites, dup.stations) #no longer needed, remove

#see if sample IDs match river mile and side
#CHECK the two columns created below for "FALSE", indicating a mismatch
#angling set to return TRUE, as angling sites do not have side, RM in site name
site$side_match <- ifelse(site$GEAR_CODE %in% c("LL", "AN"), TRUE,
                          gsub("[^[:alpha:]]",  "", site$DATASHEET_SAMPLE_ID) ==
                          site$SIDE_CODE)
site$RM_match <- ifelse(site$GEAR_CODE %in% c("LL", "AN", "MHB"), TRUE,
                        as.numeric(gsub("[[:alpha:]]", "",
                                   site$DATASHEET_SAMPLE_ID)) == site$START_RM)
#CHECK RM.sampled column for outliers (values should generally be between 0.08
#and 0.19) check outliers using mapbooks - not all outliers are errors, some
#sites are long or short due to meanders or complex shorelines
site$RM.sampled <- site$END_RM - site$START_RM

#CHECK that sample sites proceed downstream in a logical manner over time
#bad outliers in this chart are likely typos in either date or river mile
#outliers 1 day ahead of sites with like river miles are probably sites after
#midnight where we need to update date to the following day
ggplot(site[is.na(site$TRIP_ID) == FALSE,], aes(START_DATETIME, START_RM)) +
  geom_point(shape = 1, aes(color = GEAR_CODE)) +
  theme_bw() +
  scale_x_datetime(date_breaks = "2 days", date_labels = "%m-%d") +
  facet_wrap(~TRIP_ID, scales = "free_x") +
  theme(legend.position = "top")

#same as above plot, but with site names to identify problem sites
ggplot(site[is.na(site$TRIP_ID) == FALSE,], aes(START_DATETIME, START_RM)) +
  geom_text(aes(label = DATASHEET_SAMPLE_ID), size = 2) +
  theme_bw() +
  scale_x_datetime(date_breaks = "2 days", date_labels = "%m-%d") +
  facet_wrap(~TRIP_ID, scales = "free_x")


#CHECK that IDs, start and end RMs match those from GCMRC reference table
#this is not perfect - several sites showed up as mismatches even though our
#entered sites are correct (according to mapbook)
#approx RM 260 - 280 is wrong in GCMRC database - if errors show up in these RMs
#entered data is probably correct and GCMRC data wrong
station$DATASHEET_SAMPLE_ID <- gsub("_", "", station$DATASHEET_SAMPLE_ID)
site$ID.match <- ifelse(site$GEAR_CODE == "AN", TRUE,
                    site$DATASHEET_SAMPLE_ID %in% station$DATASHEET_SAMPLE_ID)

site <- merge(site, station, by = "DATASHEET_SAMPLE_ID",  all.x = TRUE)
site$start.match <- ifelse(site$GEAR_CODE == "MHB", TRUE,
                           site$START_RM == site$start_RM)
site$end.match <- ifelse(site$GEAR_CODE == "MHB", TRUE,
                         site$END_RM == site$end_RM)


#see if TURBIDITY_NTU matches categorical turbidity
#CHECK for entry errors if any site returns FALSE in turbidity.match column
site$turbidity.match <- site$TURBIDITY ==
                                 ifelse(site$TURBIDITY_NTU > 150, "H",
                                    ifelse(site$TURBIDITY_NTU < 60, "L", "M"))

#see if entered PIT tags have match in scanner downloads
#550 missing - seems like error with scanner download, not data entry
tags <- fish[!(is.na(fish$PITTAG)),] #tagged fish only
tags <- merge(x = tags, y = scan, by = "PITTAG", all.x = TRUE) #join
#CHECK tags in tag.errors dataframe for data entry errors
tag.errors <- tags[is.na(tags$date),]

#CHECK for duplicate tag entries (sometimes boatmen accidentally write down the
# same tag twice)
#Not all duplicates are errors, a fish may have been captured twice
duplicate.tags <- fish[!is.na(fish$PITTAG), ]
duplicate.tags <- duplicate.tags[duplicated(duplicate.tags$PITTAG) |
                  #check backwards too, to get original from duplicate pair
                         duplicated(duplicate.tags$PITTAG, fromLast = TRUE),]

#see if TOTAL_CATCH is correct
#calculate catch per site
fish$nfc <- ifelse(fish$SPECIES_CODE == "NFC", "NFC", "fish caught")

catch <- fish %>%
  group_by(DATASHEET_SAMPLE_ID, nfc) %>%
  summarize(n.total = length(DATASHEET_SAMPLE_ID))

catch$n.total <- ifelse(catch$nfc == "NFC", 0, catch$n.total)
catch$nfc <- NULL

catch <- catch[!duplicated(catch),] #remove duplicate rows
#join with existing data to see if total catches match
site <- merge(x = site, y = catch, by = "DATASHEET_SAMPLE_ID", all.y = TRUE)
site$catch.match <- site$n.total == site$TOTAL_CATCH
#CHECK total catch for any site with FALSE in the ERROR_CATCH column

############################ CHECK FISH DATA

#Compare total length to fork length
#CHECK for fish where FL is less than TL (TL.minus.FL is negative)
#CHECK for fish where TL is implausibly longer than FL (TL.minus.FL is very large)
fish$TL.minus.FL <- fish$TOTAL_LENGTH - fish$FORK_LENGTH

#plot TL vs. FL
#CHECK graphs for outliers
fms <- fish[fish$SPECIES_CODE == "FMS", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = fms,  main = "Flannelmouth")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = fms, labels = fms$START_RM)

plot(WEIGHT ~ TOTAL_LENGTH, data = fms,  main = "Flannelmouth")
text(WEIGHT ~ TOTAL_LENGTH, data = fms, labels = fms$START_RM)

bhs <- fish[fish$SPECIES_CODE == "BHS", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = bhs,  main = "Bluehead")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = bhs, labels = bhs$START_RM)

plot(WEIGHT ~ TOTAL_LENGTH, data = bhs,  main = "Bluehead")
text(WEIGHT ~ TOTAL_LENGTH, data = bhs, labels = bhs$START_RM)

hbc <- fish[fish$SPECIES_CODE == "HBC", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = hbc,  main = "Humpback Chub")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = hbc, labels = hbc$ACCESS_SAMPLE_ID)

plot(WEIGHT ~ TOTAL_LENGTH, data = hbc,  main = "Humpback")
text(WEIGHT ~ TOTAL_LENGTH, data = hbc, labels = hbc$START_RM)


rbt <- fish[fish$SPECIES_CODE == "RBT", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = rbt,  main = "Rainbow Trout")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = rbt, labels = rbt$START_RM)

bnt <- fish[fish$SPECIES_CODE == "BNT", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = bnt, type = "n", main = "Brown Trout")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = bnt, labels = bnt$START_RM)

crp <- fish[fish$SPECIES_CODE == "CRP", ]
plot(TOTAL_LENGTH ~ FORK_LENGTH, data = crp, type = "n", main = "Carp")
text(TOTAL_LENGTH ~ FORK_LENGTH, data = crp, labels = crp$START_RM)

#get values needed to calculate TL and FL and fix length errors
lm.fms.fl <- lm(FORK_LENGTH ~ TOTAL_LENGTH, data = fms)
summary(lm.fms.fl)

lm.fms.tl <- lm(TOTAL_LENGTH ~ FORK_LENGTH, data = fms)
summary(lm.fms.tl)

#plot length vs. wt for non-natives
#CHECK graphs for outliers
rbt <- fish[fish$SPECIES_CODE == "RBT", ]
plot(WEIGHT ~ TOTAL_LENGTH, data = rbt,  main = "Rainbow Trout")
text(WEIGHT ~ TOTAL_LENGTH, data = rbt, labels = rbt$START_RM)

bnt <- fish[fish$SPECIES_CODE == "BNT", ]
plot(WEIGHT ~ TOTAL_LENGTH, data = bnt, main = "Brown Trout")
text(WEIGHT ~TOTAL_LENGTH, data = bnt, labels = bnt$START_RM)

crp <- fish[fish$SPECIES_CODE == "CRP", ]
plot(WEIGHT ~ TOTAL_LENGTH, data = crp,  main = "Carp")
text(WEIGHT ~ TOTAL_LENGTH, data = crp, labels = crp$START_RM)

plot(WEIGHT ~ TOTAL_LENGTH, data = fms,  main = "Flannelmouth")
text(WEIGHT ~ TOTAL_LENGTH, data = fms, labels = rbt$START_RM)


plot(WEIGHT ~ TOTAL_LENGTH, data = bhs,  main = "bluehead")
text(WEIGHT ~ TOTAL_LENGTH, data = bhs, labels = rbt$START_RM)


plot(WEIGHT ~ TOTAL_LENGTH, data = hbc,  main = "humpback chub")
text(WEIGHT ~ TOTAL_LENGTH, data = hbc, labels = rbt$START_RM)

