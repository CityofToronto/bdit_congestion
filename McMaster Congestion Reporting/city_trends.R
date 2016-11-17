library(plyr)
library(data.table)
library(ggplot2)
library(reshape)
library(reshape2)
library(broom)
library(RPostgreSQL)
library(lme4)
library(stringr)

analysis_year = 2014
mi_to_m = 1609.34
avail_months <- 1:12

##############################################
# IMPORT RAW DATA FROM POSTGRESQL
##############################################

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

fileLoc = "K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash"

##############################################
# FUNCTION DEFINITIONS
##############################################

impute <- function(a, a.impute) {
  ifelse(is.na(a),a.impute,a)
}

tti_summary <- function(x, groups, days, hours) {
  result <- ddply (x,groups,here(summarise),
                   tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
                   tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
                   tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
                   tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
                   tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
                   tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
                   speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
                   speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
                   speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
                   delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(mi_to_m*days*hours),
                   delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(mi_to_m*days*hours),
                   delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(mi_to_m*days*hours),
                   delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(mi_to_m*days*hours),
                   delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(mi_to_m*days*hours),
                   delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(mi_to_m*days*hours),
                   vkt.volume=sum(volume*(Length_m/1000))/(days*hours),
                   vkt.count.adj=sum(count.adj*(Length_m/1000))/(days*hours),
                   vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(days*hours)
  )
  return(result)
}

##############################################
# IMPORT MONTHLY/HOURLY/WKDAY SPEED DATA
##############################################

# IMPORT 2014 DATA
curr_dir <- "/out/out_14/day.month/"

dat.day.month.14 <- ldply(.data = paste(getwd(),curr_dir,dir(paste(getwd(),curr_dir,sep="")),sep=""), 
                          read.table, 
                          col.names=c("id","tmc", "time.15.continuous", "year", "month", "weekday",
                                      "speed.wtd","count", "p05","speed"))

# IMPORT 2013 DATA
curr_dir <- "/out/out_13/day.month/"

dat.day.month.13 <- ldply(.data = paste(getwd(),curr_dir,dir(paste(getwd(),curr_dir,sep="")),sep=""), 
                          read.table, 
                          col.names=c("id","tmc", "time.15.continuous", "year", "month", "weekday",
                                      "speed.wtd","count", "p05","speed"))

# IMPORT 2011 DATA
curr_dir <- "/out/out_11/day.month/"

dat.day.month.11 <- ldply(.data = paste(getwd(),curr_dir,dir(paste(getwd(),curr_dir,sep="")),sep=""), 
                          read.table, 
                          col.names=c("id","tmc", "time.15.continuous", "year", "month", "weekday",
                                      "speed.wtd","count", "p05","speed"))


source("import_ref.R")

# NOTE THAT THE 2014 TRAFFIC VOLUMES ARE BASED ON A LINEAR TREND INTERPRETATION BETWEEN 2011 AND 2016 TRAFFIC RESULTS.
# THE UNITS HERE ARE VOLUMES PER HOUR.  

curr_dir <- "in/"

########
# 2014 #
########

dat.14a <-
    read.table(paste(curr_dir, "final_net2014.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.14a <-
    melt(dat.14a, id="newADD")
colnames(dat.14a) <- c("newADD", "hour", "volume")

dat.14a$newADD      <- as.numeric(dat.14a$newADD)
dat.14a$hour        <- as.numeric(dat.14a$hour)
dat.14a$volume      <- as.numeric(dat.14a$volume)

########
# 2013 #
########
dat.13a <-
  read.table(paste(curr_dir, "final_net2013.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.13a <-
  melt(dat.13a, id="newADD")
colnames(dat.13a) <- c("newADD", "hour", "volume")

dat.13a$newADD      <- as.numeric(dat.13a$newADD)
dat.13a$hour        <- as.numeric(dat.13a$hour)
dat.13a$volume      <- as.numeric(dat.13a$volume)

########
# 2011 #
########
dat.11a <-
  read.table(paste(curr_dir, "final_net2011.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.11a <-
  melt(dat.11a, id="newADD")
colnames(dat.11a) <- c("newADD", "hour", "volume")

dat.11a$newADD      <- as.numeric(dat.11a$newADD)
dat.11a$hour        <- as.numeric(dat.11a$hour)
dat.11a$volume      <- as.numeric(dat.11a$volume)

# CLEAN UP COLUMN NAMES
colnames(torNetwork_UIDs_feb10)[colnames(torNetwork_UIDs_feb10)=="Tmc"] <- "tmc"
colnames(dat.final.net)[colnames(dat.final.net)=="Tmc"] <- "tmc"

##############################################
# READ IN MTO DATA
##############################################

dat.mto.vol <- 
    read.table(paste(curr_dir,"mto_04-14-2015.txt",sep=""), 
    col.names = c("mto.location.id", "weekday", "hour", "mto.volume", "date", "freeway"))

dat.mto.vol.lookup <- 
    read.table(file = paste(curr_dir,"mto_04-14-2015_lookup.txt",sep=""),
               col.names = c("mto.description", "mto.location.id", "newADD", "vds.tvis"))

dat.mto.vol.lookup <- dat.mto.vol.lookup[,c("mto.location.id", "newADD")]
dat.mto.vol <- merge(dat.mto.vol, dat.mto.vol.lookup, by = "mto.location.id")
rm(dat.mto.vol.lookup)

################################
save.image("CITY_TRENDS.RData")
################################

##############################################
# LOAD (OR CALCULATE) VOLUME ADJUSTMENT MODEL
##############################################

load(paste(curr_dir, "vol_adj.rda",sep=""))

adj <- data.frame(tidy(summary(lm.11a))[1:2])
adj.hr.wkdybin <- data.frame(t(data.frame(strsplit(adj[13:nrow(adj),1],":"))))
rownames(adj.hr.wkdybin) <- NULL
adj.hr.wkdybin$X1 <- NULL
adj.hr.wkdybin$X3 <- str_sub(as.character(adj.hr.wkdybin$X3),start=-1)
adj.hr.wkdybin$X2 <- str_sub(as.character(adj.hr.wkdybin$X2),start=16)
names(adj.hr.wkdybin) <- c("hour","weekday.bin")
adj.hr.wkdybin$factor <- adj[13:nrow(adj),2] 
adj.hr.wkdybin$factor <- adj.hr.wkdybin$factor + mean(adj[1:12,2])

##############################################
# VOLUME ADJUSTMENTS
##############################################

########
# 2014 #
########
hour <- floor(dat.day.month.14$time.15.continuous/10)
dat.14 <- data.frame(cbind(dat.day.month.14, hour))
rm(hour)

dat.14 <- subset(dat.14, subset = (hour>4&hour<22))
dat.14 <- 
    ddply(.data = dat.14,
          . (tmc, month, weekday, hour),
          .fun = summarise,
          speed.wtd=sum(speed.wtd*count)/sum(count)
      )  
dat.14 <- unique(dat.14)

dat.14 <- merge(dat.final.net, dat.14, by = "tmc", all = FALSE)
dat.14 <- merge(dat.14, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.14 <- merge(dat.14, dat.speed85, by = "tmc" , all = FALSE)
dat.14 <- merge(dat.14, dat.14a, by = c("hour", "newADD"), all = FALSE)

dat.14$weekday.bin <- 1
dat.14$weekday.bin[dat.14$weekday==1| dat.14$weekday==7] <- 0

# COUNT ADJUSTMENTS ARE CALCULATED HERE.
dat.14$count.adj <- 4*predict(lm.11a, dat.14)

# PROCESSING SO THAT DELAY IS NEVER NEGATIVE
dat.14$speed.wtd1 <- pmin(dat.14$speed.wtd, dat.14$speed85)
dat.14$speed.wtd2 <- pmin(dat.14$speed.wtd, dat.14$night.speed)

################################
save.image("CITY_TRENDS.RData")
################################

########
# 2013 #
########
hour <- floor(dat.day.month.13$time.15.continuous/10)
dat.13 <- data.frame(cbind(dat.day.month.13, hour))
rm(hour)

dat.13 <- subset(dat.13, subset = (hour>4&hour<22))
dat.13 <- 
  ddply(.data = dat.13,
        . (tmc, month, weekday, hour),
        .fun = summarise,
        speed.wtd=sum(speed.wtd*count)/sum(count)
  )  
dat.13 <- unique(dat.13)

dat.13 <- merge(dat.final.net, dat.13, by = "tmc", all = FALSE)
dat.13 <- merge(dat.13, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.13 <- merge(dat.13, dat.speed85, by = "tmc" , all = FALSE)
dat.13 <- merge(dat.13, dat.13a, by = c("hour", "newADD"), all = FALSE)

dat.13$weekday.bin <- 1
dat.13$weekday.bin[dat.13$weekday==1| dat.13$weekday==7] <- 0

# COUNT ADJUSTMENTS ARE CALCULATED HERE.
dat.13$count.adj <- 4*predict(lm.11a, dat.13)

# PROCESSING SO THAT DELAY IS NEVER NEGATIVE
dat.13$speed.wtd1 <- pmin(dat.13$speed.wtd, dat.13$speed85)
dat.13$speed.wtd2 <- pmin(dat.13$speed.wtd, dat.13$night.speed)

################################
save.image("CITY_TRENDS.RData")
################################

########
# 2011 #
########
hour <- floor(dat.day.month.11$time.15.continuous/10)
dat.11 <- data.frame(cbind(dat.day.month.11, hour))
rm(hour)

dat.11 <- subset(dat.11, subset = (hour>4&hour<22))
dat.11 <- 
  ddply(.data = dat.11,
        . (tmc, month, weekday, hour),
        .fun = summarise,
        speed.wtd=sum(speed.wtd*count)/sum(count)
  )  
dat.11 <- unique(dat.11)

dat.11 <- merge(dat.final.net, dat.11, by = "tmc", all = FALSE)
dat.11 <- merge(dat.11, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.11 <- merge(dat.11, dat.speed85, by = "tmc" , all = FALSE)
dat.11 <- merge(dat.11, dat.11a, by = c("hour", "newADD"), all = FALSE)

dat.11$weekday.bin <- 1
dat.11$weekday.bin[dat.11$weekday==1| dat.11$weekday==7] <- 0

# COUNT ADJUSTMENTS ARE CALCULATED HERE.
dat.11$count.adj <- 4*predict(lm.11a, dat.11)

# PROCESSING SO THAT DELAY IS NEVER NEGATIVE
dat.11$speed.wtd1 <- pmin(dat.11$speed.wtd, dat.11$speed85)
dat.11$speed.wtd2 <- pmin(dat.11$speed.wtd, dat.11$night.speed)

################################
save.image("CITY_TRENDS.RData")
################################

##############################################
# MTO ADJUSTMENTS
##############################################
dat.mto.vol$date <- as.Date(dat.mto.vol$date, format = "%d-%m-%Y")
dat.mto.vol$month <- as.numeric(format(dat.mto.vol$date, format = "%m"))
dat.mto.vol$year <- 2013
dat.mto.vol$year[dat.mto.vol$mto.location.id > 28] <- 2014


##############################################
# MTO VOLUME ADJUSTMENTS - MODEL TESTING
##############################################
dat.13a <- dat.13[,c("weekday", "month", "hour", "newADD", "count.adj", "CorridorUID")]
dat.13a$year <- 2013

dat.14a<-dat.14[,c("weekday", "month", "hour", "newADD", "count.adj", "CorridorUID")]
dat.14a$year <- 2014

dat.13a.14a <- data.frame(rbind(dat.13a, dat.14a))
rm(dat.13a, dat.14a)

dat.13a.14a <- 
    merge(x = dat.mto.vol, 
          y = dat.13a.14a, 
          by = c("weekday", "month", "hour", "newADD", "year"), 
          all = FALSE)

dat.13a.14a <- rename(dat.13a.14a, c("CorridorUID"="CorridorUID1"))

lm.b <- 
    lm(formula = mto.volume~as.factor(CorridorUID1)
                  + as.factor(weekday!=1 & weekday !=7)
                  + count.adj:as.factor(weekday!=1 & weekday !=7)
                  + count.adj:as.factor(CorridorUID1), 
       data = dat.13a.14a)
tidy(lm.b)

lm.c <- 
    lm(formula = mto.volume ~ as.factor(weekday!=1 & weekday !=7)
                  + count.adj:as.factor(weekday!=1 & weekday !=7 ), 
       data = dat.13a.14a)
tidy(lm.c)

##############################################
# INTEGRATION OF MTO VOLUME ADJUSTMENTS
##############################################

#80 - HWY 400
#81 - HWY 401 Collectors
#82 - HWY 401 Express
#83 - HWY 409
#85 - HWY 404
#87 - Allen Expwy
#88 - HWY 427
#89 - HWY 427 Collectors
mto_corridors = c(80,81,82,83,85,88)
oth_corridors = c(87,89)

########
# 2014 #
########
dat.14$CorridorUID1 <- dat.14$CorridorUID
dat.14$CorridorUID1[!(dat.14$CorridorUID %in% mto_corridors)] <- 88

dat.14$count.adj.all <- 
    dat.14$count.adj
dat.14$count.adj.all[dat.14$CorridorUID %in% mto_corridors] <- 
    predict(lm.b, dat.14[dat.14$CorridorUID %in% mto_corridors,])
dat.14$count.adj.all[dat.14$CorridorUID %in% mto_corridors & dat.14$count.adj.all < 1] <- 
    0
dat.14$count.adj.all[dat.14$CorridorUID %in% oth_corridors] <- 
   predict(lm.c, dat.14[dat.14$CorridorUID %in% oth_corridors,])


########
# 2013 #
########
dat.13$CorridorUID1 <- dat.13$CorridorUID
dat.13$CorridorUID1[!(dat.13$CorridorUID %in% mto_corridors)] <- 88

dat.13$count.adj.all <- 
    dat.13$count.adj
dat.13$count.adj.all[dat.13$CorridorUID %in% mto_corridors] <- 
    predict(lm.b, dat.13[dat.13$CorridorUID %in% mto_corridors,])
dat.13$count.adj.all[dat.13$CorridorUID %in% mto_corridors & dat.13$count.adj.all < 1] <- 
    0
dat.13$count.adj.all[dat.13$CorridorUID %in% oth_corridors] <- 
    predict(lm.c, dat.13[dat.13$CorridorUID %in% oth_corridors,])


########
# 2011 #
########
dat.11$CorridorUID1 <- dat.11$CorridorUID
dat.11$CorridorUID1[!(dat.11$CorridorUID %in% mto_corridors)] <- 88

dat.11$count.adj.all <- 
    dat.11$count.adj
dat.11$count.adj.all[dat.11$CorridorUID %in% mto_corridors] <- 
    predict(lm.b, dat.11[dat.11$CorridorUID %in% mto_corridors,])
dat.11$count.adj.all[dat.11$CorridorUID %in% mto_corridors & dat.11$count.adj.all < 1] <- 
    0
dat.11$count.adj.all[dat.11$CorridorUID %in% oth_corridors] <- 
    predict(lm.c, dat.11[dat.11$CorridorUID %in% oth_corridors,])
rm(dat.11a)


##############################################
# VOLUME WEIGHT ADJUSTMENTS
##############################################

########
# 2014 #
########
dat.14$weight.vol <- (dat.14$volume*dat.14$Length_m) / mean(dat.14$volume*dat.14$Length_m)
dat.14$weight.adj <- (dat.14$count.adj*dat.14$Length_m) / mean(dat.14$count.adj*dat.14$Length_m)
dat.14$weight.adj.all <- (dat.14$count.adj.all*dat.14$Length_m)/mean(dat.14$count.adj.all*dat.14$Length_m)

########
# 2013 #
########
dat.13$weight.vol <- (dat.13$volume*dat.13$Length_m) / mean(dat.13$volume*dat.13$Length_m)
dat.13$weight.adj <- (dat.13$count.adj*dat.13$Length_m) / mean(dat.13$count.adj*dat.13$Length_m)
dat.13$weight.adj.all <- (dat.13$count.adj.all*dat.13$Length_m)/mean(dat.13$count.adj.all*dat.13$Length_m)

########
# 2011 #
########
dat.11$weight.vol <- (dat.11$volume*dat.11$Length_m) / mean(dat.11$volume*dat.11$Length_m)
dat.11$weight.adj <- (dat.11$count.adj*dat.11$Length_m) / mean(dat.11$count.adj*dat.11$Length_m)
dat.11$weight.adj.all <- (dat.11$count.adj.all*dat.11$Length_m)/mean(dat.11$count.adj.all*dat.11$Length_m)

rm(dat.final.net, dat.mto.vol, dat.speed85, dat.tmc.newADD, torNetwork_UIDs_feb10)

################################
save.image("CITY_TRENDS.RData")
################################

##############################################
# MODELING
##############################################

lmer.11b <- lmer( speed.wtd ~ 1 
                  + (1|tmc:hour:weekday) 
                  + (1|month), 
               data = dat.11, 
               weights = weight.adj)

lmer.13b <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
               data = dat.13, 
               weights = weight.adj)

lmer.14b <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
               data = dat.14, 
               weights = weight.adj)

tidy(lmer.14b)
fixef(lmer.14b)
ranef(lmer.14b)

tidy(lmer.13b)
fixef(lmer.13b)
ranef(lmer.13b)

tidy(lmer.11b)
fixef(lmer.11b)
ranef(lmer.11b)

save.image("CITY_TRENDS.RData")

##############################
# ADJUSTED BY weight.adj.all #
##############################

lmer.11c <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
               data = dat.11, 
               weights = weight.adj.all)

lmer.13c <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
               data = dat.13, 
               weights = weight.adj.all)

lmer.14c <- lmer( speed.wtd~1
                  +(1|tmc:hour:weekday)
                  + (1|month), 
               data = dat.14, 
               weights = weight.adj.all)

lmer.14d <- lmer( speed.wtd~1
                  + (1|tmc:hour:weekday)
                  + (1|month) 
                  + (1|hour), 
               data = dat.14, 
               weights = weight.adj.all, 
               subset = weekday.bin == 1)

#################################
save.image("CITY_TRENDS.RData")
#################################

dat.11$year <- 2011
dat.13$year <- 2013
dat.14$year <- 2014
dat.all <- data.frame(rbind(dat.11, dat.13, dat.14))

#################################
save.image("CITY_TRENDS.RData")
#################################

# FREEWAY MODEL ONLY
lmer.all <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month)
                  + (1|year), 
               data = dat.all, 
               weights = weight.adj.all, 
               subset = (Freeway == 1))
tidy(lmer.all)
fixef(lmer.all)
ranef(lmer.all)

#################################
save.image("CITY_TRENDS_02.RData")
#################################

# FREEWAY MODEL
lmer.fre.year.hourly <- lmer( speed.wtd ~ 1
                              + (1|tmc:hour)
                              + (1|month)
                              + (1|year:hour), 
               data = dat.all, 
               weights = weight.adj.all, 
               subset = (Freeway == 1 & weekday.bin == 1))
tidy(lmer.fre.year.hourly)
fixef(lmer.fre.year.hourly)
ranef(lmer.fre.year.hourly)

# ARTERIAL MODEL
lmer.art.year.hourly <- lmer( speed.wtd ~ 1
                              + (1|tmc:hour)
                              + (1|month)
                              + (1|year:hour), 
                           data = dat.all, 
                           weights = weight.adj.all, 
                           subset = (Freeway != 1 & weekday.bin == 1))
tidy(lmer.art.year.hourly)
fixef(lmer.art.year.hourly)
ranef(lmer.art.year.hourly)

#################################
save.image("CITY_TREND_02.RData")
#################################

#EXTRACTING THE RESULTS FOR FURTHER PROCESSING:
summary.lmer.14b.month <- as.data.frame(ranef(lmer.14b)[2])
summary.lmer.14b <- as.data.frame(ranef(lmer.14b)[1])
colnames(summary.lmer.14b) <- "speed"
colnames(summary.lmer.14b.month) <- "speed"

summary.lmer.14b <- data.frame(cbind(
  colsplit(row.names(summary.lmer.14b),":", c("tmc", "hour", "weekday")), 
  summary.lmer.14b$speed))
summary.lmer.14b.month <- data.frame(cbind(
  row.names(summary.lmer.14b.month),
  summary.lmer.14b.month$speed))

names(summary.lmer.14b.month) <- c("month","speed")
names(summary.lmer.14b) <- c("tmc","hour","weekday","speed")

summary.lmer.14b.month$speed <- as.numeric(as.character(summary.lmer.14b.month$speed))*1.60934
summary.lmer.14b$speed <- as.numeric(as.character(summary.lmer.14b$speed))*1.60934

source("import_ref.R")

summary.lmer.14b <- merge(summary.lmer.14b, dat.tmc.newADD, by = "tmc", all = FALSE)
summary.lmer.14b$newADD <- as.numeric(summary.lmer.14b$newADD)
summary.lmer.14b <- merge(summary.lmer.14b, dat.speed85, by = "tmc" , all = FALSE)
summary.lmer.14b <- merge(summary.lmer.14b, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.final.net1 <- dat.final.net[,c("newADD", "Freeway", "Length_m")]
summary.lmer.14b <- merge(summary.lmer.14b, dat.final.net1, by = "newADD", all = FALSE)

dat.14.vol <- unique(dat.14[c("newADD","hour","volume")])
summary.lmer.14b <- merge(summary.lmer.14b, dat.14.vol, by = c("hour", "newADD"), all = FALSE)
rm(dat.final.net1)

summary.lmer.14b$weekday.bin <- 1
summary.lmer.14b$weekday.bin[summary.lmer.14b$weekday==1 | summary.lmer.14b$weekday==7] <- 0

summary.lmer.14b$speed85<-summary.lmer.14b$speed85*1.60934
summary.lmer.14b$night.speed<-summary.lmer.14b$night.speed*1.60934


# COUNT ADJUSTMENTS ARE CALCULATED HERE.  
for (i in avail_months){
  summary.lmer.14b$month <- i
  if (i == 1){
    count.adj <- 4 * predict(lm.11a, summary.lmer.14b)
  }
  else {
    count.adj <- count.adj + 4 * predict(lm.11a, summary.lmer.14b)
  }
}

summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, count.adj))


# PROCESSING SO THAT DELAY IS NEVER NEGATIVE
summary.lmer.14b$speed1 <- pmin(summary.lmer.14b$speed, summary.lmer.14b$speed85)
summary.lmer.14b$speed2 <- pmin(summary.lmer.14b$speed, summary.lmer.14b$night.speed)

summary.lmer.14b$vkt.count.adj <- 
  summary.lmer.14b$count.adj * (summary.lmer.14b$Length_m/1000)
summary.lmer.14b$vkt.count.adj.speed <- 
  summary.lmer.14b$vkt.count.adj/summary.lmer.14b$speed
summary.lmer.14b$vkt.count.adj.speed1 <- 
  summary.lmer.14b$vkt.count.adj/summary.lmer.14b$speed1
summary.lmer.14b$vkt.count.adj.speed2 <- 
  summary.lmer.14b$vkt.count.adj/summary.lmer.14b$speed2
summary.lmer.14b$vkt.count.adj.speed85 <- 
  summary.lmer.14b$vkt.count.adj/summary.lmer.14b$speed85
summary.lmer.14b$vkt.count.adj.night.speed <- 
  summary.lmer.14b$vkt.count.adj/summary.lmer.14b$night.speed

#INTEGRATING MTO ADJUSTMENTS FOR 2011.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####

summary.lmer.14b$CorridorUID1 <- summary.lmer.14b$CorridorUID
summary.lmer.14b$CorridorUID1[summary.lmer.14b$CorridorUID1 %in% mto_corridors] <- 88

summary.lmer.14b$count.adj.all <- summary.lmer.14b$count.adj
summary.lmer.14b$count.adj.all[summary.lmer.14b$CorridorUID %in% mto_corridors] <- 
  predict(lm.b, summary.lmer.14b[summary.lmer.14b$CorridorUID %in% mto_corridors,])
summary.lmer.14b$count.adj.all[summary.lmer.14b$CorridorUID %in% mto_corridors & summary.lmer.14b$count.adj.all < 1] <- 
  0
summary.lmer.14b$count.adj.all[summary.lmer.14b$CorridorUID %in% oth_corridors] <- 
  predict(lm.c, summary.lmer.14b[summary.lmer.14b$CorridorUID %in% oth_corridors,])

dat.temp <- subset(summary.lmer.14b, subset = (weekday.bin == 1& Freeway ==1))
out.lmer.14b.daily <- ddply ( dat.temp, 
                            . (hour),
                            summarise,
                           vkt.k=sum(vkt.count.adj),
                           delay.85.hrs=sum(vkt.count.adj.speed1-vkt.count.adj.speed85),
                           delay.night.hrs=sum(vkt.count.adj.speed2-vkt.count.adj.night.speed),
                           tti.85=(sum(vkt.count.adj.speed1)/sum(vkt.count.adj.speed85)),
                           tti.night=(sum(vkt.count.adj.speed2)/sum(vkt.count.adj.night.speed)),
                           speed=sum(speed*vkt.count.adj)/sum(vkt.count.adj)
                           )


lmer.11e <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month),
                  data = dat.11,
                  weights = weight.adj)

lmer.13e <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
                  data = dat.13,
                  weights = weight.adj)

lmer.14e <- lmer( speed.wtd ~ 1
                  + (1|tmc:hour:weekday)
                  + (1|month), 
                  data = dat.14,
                  weights = weight.adj)

#################################
save.image("CITY_TREND_04.RData")
#################################

#######################################
# CORRIDOR TRAVEL TIME INDEX SUMMARIES
#######################################

########
# 2014 #
########

dat.temp <- subset(dat.14, subset = weekday.bin == 1)

dat.temp$C_UID[dat.temp$C_UID==29.1] <- 84.1
dat.temp$C_UID[dat.temp$C_UID==29.2] <- 84.2
dat.temp$C_UID[dat.temp$C_UID==29.3] <- 84.3
dat.temp$C_UID[dat.temp$C_UID==29.4] <- 84.4
corridor.direct.hourly.perf.14 <- tti_summary(dat.temp, c("hour", "C_UID"), 5, 12)

dat.temp <- subset(dat.14, subset = weekday.bin ==1 )
dat.temp$CorridorUID [dat.temp$CorridorUID==29] <- 84
corridor.hourly.perf.14 <- tti_summary(dat.temp, c("hour", "CorridorUID"), 5, 12)

dat.temp <- subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
system.hourly.perf.14 <- tti_summary(dat.temp, c("hour"), 5, 12)

dat.temp <- subset(dat.14, subset = weekday.bin == 0)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
system.hourly.perf.14.weekend <- tti_summary(dat.temp, c("hour"), 2, 12)

dat.temp <- subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.14 <- tti_summary(dat.temp, c("hour", "Freeway"), 5, 12)

# FIGURE 10: CITY-WIDE HOURLY TRAVEL DELAY PROFILE
ggplot(data = subset(fre.art.hourly.perf.14), 
       aes(x = hour, y = delay.85.count.adj.all, 
           group = as.factor(Freeway), col = as.factor(Freeway))) +
  geom_line() +
  scale_y_continuous(limits = c(0,12000), expand = c(0,0))

dat.temp <- subset(dat.14, subset = weekday.bin == 0)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.14.weekend <- tti_summary(dat.temp, c("hour", "Freeway"), 2, 12)

dat.temp <- subset(dat.14)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.14.weekday.bin <- tti_summary(dat.temp, c("hour", "Freeway","weekday.bin"), 1, 12)

# FIGURE 12: CITY-WIDE MEAN FREEWAY SPEEDS FOR TYPICAL WEEKDAY AND WEEKEND
ggplot(data = subset(fre.art.hourly.perf.14.weekday.bin, subset = Freeway == 1), 
       aes(x = hour, y = speed.count.adj.all*1.60934, 
           group = as.factor(weekday.bin), col = as.factor(weekday.bin))) +
  geom_line() +
  scale_y_continuous(limits = c(60,105), expand = c(0,0))


# FIGURE 13: CITY-WIDE MEAN ARTERIAL SPEEDS FOR TYPICAL WEEKDAY AND WEEKEND
ggplot(data = subset(fre.art.hourly.perf.14.weekday.bin, subset = Freeway == 0), 
       aes(x = hour, y = speed.count.adj.all*1.60934, 
           group = as.factor(weekday.bin), col = as.factor(weekday.bin))) +
  geom_line() +
  scale_y_continuous(limits = c(30,55), expand = c(0,0))


################################################
# HOURLY SPEEDS BY YEAR (SEPTEMBER TO NOVEMBER)
################################################

dat.temp <- subset(dat.13, subset = weekday.bin == 1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.13.sep.nov <- tti_summary(dat.temp, c("hour", "Freeway"), 5, 3)
fre.art.hourly.perf.13.sep.nov$year <- 2013

dat.temp <- subset(dat.11, subset = weekday.bin == 1& (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.11.sep.nov <- tti_summary(dat.temp, c("hour", "Freeway"), 5, 3)
fre.art.hourly.perf.11.sep.nov$year <- 2011

dat.temp <- subset(dat.14, subset = weekday.bin == 1& (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.hourly.perf.14.sep.nov <- tti_summary(dat.temp, c("hour", "Freeway"), 5, 3)
fre.art.hourly.perf.14.sep.nov$year <- 2014

fre.art.hourly.perf.all.sep.nov <- rbind(fre.art.hourly.perf.11.sep.nov,
                                         fre.art.hourly.perf.13.sep.nov,
                                         fre.art.hourly.perf.14.sep.nov)

write.csv(fre.art.hourly.perf.all.sep.nov, file = "sep_nov.csv")

# FIGURE 9: CITY-WIDE HOURLY MEAN FREEWAY SPEEDS (2011, 2013, 2014)
ggplot(data = subset(fre.art.hourly.perf.all.sep.nov, subset = Freeway == 1), 
                     aes(x = hour, y = speed.count.adj.all*1.60934, 
                                                   group = as.factor(year), col = as.factor(year))) +
         geom_line() +
  scale_y_continuous(limits = c(65,105), expand = c(0,0))

# FIGURE 10: CITY-WIDE HOURLY MEAN ARTERIAL SPEEDS (2011, 2013, 2014)
ggplot(data = subset(fre.art.hourly.perf.all.sep.nov, subset = Freeway == 0), 
       aes(x = hour, y = speed.count.adj.all*1.60934, 
           group = as.factor(year), col = as.factor(year))) +
  geom_line() +
  scale_y_continuous(limits = c(30,55), expand = c(0,0))


dat.temp <- subset(dat.14, subset = hour == 17)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.daily.perf.14.5pm <- tti_summary(dat.temp, c("weekday", "Freeway"), 1, 12)
write.csv(fre.art.hourly.perf.all.sep.nov, file = "weekday5pm.csv")

# FIGURE 19: DOWNTOWN CORE VARIATIONS IN PEAK-HOUR SPEEDS FOR DAYS OF THE WEEK (2014)
ggplot(data = fre.art.daily.perf.14.5pm,aes(x = as.factor(weekday), y = speed.count.adj.all*1.60934, 
                                        group = as.factor(Freeway), fill = as.factor(Freeway))) +
  geom_bar(stat = "identity",position = "dodge") +
  scale_y_continuous(limits = c(0,100), expand = c(0,0))

dat.temp <- (dat.14)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.daily.perf.14 <- tti_summary(dat.temp, c("weekday", "Freeway"), 1, 12)

dat.temp <- subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.seasonal.perf.14 <- tti_summary(dat.temp, c("month", "Freeway"), 5, 1)

dat.temp <- subset(dat.14, subset = weekday.bin == 1 & hour == 17)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.seasonal.perf.14.5pm <- tti_summary(dat.temp, c("month", "Freeway"), 5, 1)

dat.temp<-subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
fre.art.seasonal.hourly.perf.14 <- tti_summary(dat.temp, c("month", "Freeway", "hour"), 5, 1)



##############################
# DOWNTOWN TRENDS
##############################
# ALL THE 1S ARE DOWNTOWN.  ALL ELSE (EVEN IF > 0) ARE NOT.

# HOURLY SPEEDS - DOWNTOWN - BY YEAR
dat.temp<-subset(dat.11, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
downtown.hourly.perf.11.sep.nov <- tti_summary(dat.temp, c("hour"), 5, 3)
downtown.hourly.perf.11.sep.nov$year <- 2011

dat.temp<-subset(dat.13, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.perf.13.sep.nov <- tti_summary(dat.temp, c("hour"), 5, 3)
downtown.hourly.perf.13.sep.nov$year <- 2013

dat.temp<-subset(dat.14, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
downtown.hourly.perf.14.sep.nov <- tti_summary(dat.temp, c("hour"), 5, 3)
downtown.hourly.perf.14.sep.nov$year <- 2014

downtown.hourly.perf.all.sep.nov <- rbind(downtown.hourly.perf.11.sep.nov,
                                          downtown.hourly.perf.13.sep.nov,
                                          downtown.hourly.perf.14.sep.nov)
write.csv(downtown.hourly.perf.all.sep.nov, file = "sep_nov_downtown.csv")



# FIGURE 14: DOWNTOWN HOURLY MEAN SPEEDS FOR ARTERIALS (2011, 2013, and 2014)
ggplot(data = subset(downtown.hourly.perf.all.sep.nov), 
       aes(x = hour, y = speed.count.adj.all*1.60934, 
           group = as.factor(year), col = as.factor(year))) +
  geom_line() +
  scale_y_continuous(limits = c(15,40), expand = c(0,0))


# FIGURE 15: DOWNTOWN TORONTO HOURLY TRAVEL DELAY PROFILE (2014)
ggplot(data = subset(downtown.hourly.perf.14.sep.nov), 
       aes(x = hour, y = delay.85.count.adj.all)) +
  geom_line() +
  scale_y_continuous(limits = c(0,1200), expand = c(0,0))


dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.weekday.perf.14 <- tti_summary(dat.temp, c("weekday"), 1, 12)
downtown.weekday.perf.14$time <- "DAILY"

dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1 & hour ==17)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.weekday.perf.14.5pm <- tti_summary(dat.temp, c("weekday"), 1, 12)
downtown.weekday.perf.14.5pm$time <- "5PM"

# FIGURE 19: DOWNTOWN CORE VARIATIONS IN PEAK-HOUR SPEEDS FOR DAYS OF THE WEEK (2014)
ggplot(data = rbind(downtown.weekday.perf.14, downtown.weekday.perf.14.5pm),
  aes(x = weekday, y = speed.count.adj.all*1.60934, group = time, fill = time)) +
  geom_bar(stat = "identity",position = "dodge") +
  scale_y_continuous(limits = c(0,35), expand = c(0,0))


  

dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.weekday.bin.perf.14 <- tti_summary(dat.temp, c("weekday.bin","hour"), 1, 12)

# FIGURE 16: DOWNTOWN MEAN SPEEDS FOR TYPICAL WEEKDAY vs. WEEKEND (2014)
ggplot(data = downtown.hourly.weekday.bin.perf.14,
       aes(x = hour, y = speed.count.adj.all*1.60934, 
           group = as.factor(weekday.bin), colour = as.factor(weekday.bin))) +
  geom_line() +
  scale_x_continuous(limits = c(5,21)) +
  scale_y_continuous(limits = c(10,35))


##############################
# YEAR OVER YEAR MODELLING
############################## 

dat.temp <- subset(dat.14, subset = (Downtown_P == 1) & (Freeway != 1) & (weekday.bin == 1))
lmer.14.downtown.seasonal <- lmer( speed.wtd ~ 1
                                   + (1|tmc:hour:weekday)
                                   + (1|month),
                                   data = dat.temp, 
                                   weights = weight.adj)


dat.temp <- subset(dat.14, subset = (Downtown_P == 1) & (Freeway !=1) & (weekday.bin == 1))
lmer.14.downtown.seasonal.hourly <- lmer( speed.wtd ~ 1
                                          + (1|tmc:hour:weekday)
                                          + (1|month:hour),
                                          data = dat.temp, 
                                          weights = weight.adj)


dat.temp<-subset(dat.14, subset = (Downtown_P == 1) & (Freeway != 1))
lmer.14.downtown.weekday <- lmer( speed.wtd ~ 1
                                  + (1|tmc:month:hour)
                                  + (1|weekday),
                                  data = dat.temp,
                                  weights = weight.adj)


dat.temp <- subset(dat.14, subset = (Downtown_P == 1) & (Freeway != 1)  & (hour == 17))
lmer.14.downtown.weekday.5pm <- lmer( speed.wtd ~ 1
                                      + (1|tmc:month)
                                      + (1|weekday),
                                      data = dat.temp, 
                                      weights = weight.adj)


lmer.downtown.all <- lmer( speed.wtd ~ 1
                           + (1|tmc:hour)
                           + (1|month)
                           + (1|year:hour),
                           data = dat.all,
                           weights = weight.adj.all,
                           subset = ((Freeway != 1) & (Downtown_P == 1) 
                                     & (weekday.bin == 1) & (month>8 & month<12)))


lmer.corridor.downtown.all <- lmer( speed.wtd ~ 1 
                                    + (1|tmc:hour)
                                    + (1|month)
                                    + (1|CorridorUID:year:hour),
                                    data = dat.all,
                                    weights = weight.adj.all,
                                    subset = ((Freeway != 1) & (Downtown_P == 1)
                                             & (CorridorUID > 0) & (weekday.bin == 1) & (month>8 & month<12)))


lmer.downtown.all.weekday.hourly <- lmer( speed.wtd ~ 1
                                          + (1|tmc:hour:weekday.bin)
                                          + (1|month)
                                          + (1|year:hour:weekday.bin), 
                                          data = dat.all, 
                                          weights = weight.adj.all, 
                                          subset = ((Freeway != 1) & (Downtown_P == 1)
                                                    & (CorridorUID > 0) & (month > 8 & month < 12)))


lmer.fre.year.hourly <- lmer( speed.wtd ~ 1
                              + (1|tmc:hour)
                              + (1|month)
                              + (1|year:hour),
                              data = dat.all, 
                              weights = weight.adj.all, 
                              subset = ((Freeway == 1) & (weekday.bin == 1)))


lmer.downtown.all1 <- lmer( speed.wtd ~ 1
                            + (1|tmc:hour:year)
                            + (1|month),
                            data = dat.all,
                            weights = weight.adj.all, 
                            subset = ((Freeway != 1) & (Downtown_P == 1)
                                     & (weekday.bin == 1) & (month>8 & month<12)))


# RESULTS ANALYSIS - LMER.DOWNTOWN.ALL.1
summary.lmer.downtown.all1 <- as.data.frame(t(as.data.frame(
  strsplit(rownames(ranef(lmer.downtown.all1)$"tmc:hour:year"),":"))))
row.names(summary.lmer.downtown.all1) <- NULL
summary.lmer.downtown.all1 <- cbind(summary.lmer.downtown.all1,
                                          data.frame(ranef(lmer.downtown.all1)$"tmc:hour:year", row.names = NULL))
names(summary.lmer.downtown.all1) <- c("tmc", "hour", "year", "speed")
summary.lmer.downtown.all1$speed<-summary.lmer.downtown.all1$speed*1.60934

summary.lmer.downtown.all1.month <- data.frame(
  month = rownames(ranef(lmer.downtown.all1)$month),
  speed = ranef(lmer.downtown.all1)$month)
row.names(summary.lmer.downtown.all1.month) <- NULL
names(summary.lmer.downtown.all1.month) <- c("month","speed")
summary.lmer.downtown.all1.month$speed <- summary.lmer.downtown.all1.month$speed*1.60934

source("import_ref.R")
summary.lmer.downtown.all1 <- merge(summary.lmer.downtown.all1, dat.tmc.newADD, by = "tmc", all = FALSE)
summary.lmer.downtown.all1$newADD <- as.numeric(summary.lmer.downtown.all1$newADD)
summary.lmer.downtown.all1 <- merge(summary.lmer.downtown.all1, dat.speed85, by = "tmc" , all = FALSE)
summary.lmer.downtown.all1 <- merge(summary.lmer.downtown.all1, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.final.net1<-dat.final.net[,c("newADD", "Freeway", "Length_m")]
summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, dat.final.net1, by = "newADD", all = FALSE)
rm(dat.final.net1)

dat.14.long <- unique(dat.14[c("newADD","year","hour","volume")])
dat.13.long <- unique(dat.13[c("newADD","year","hour","volume")])
dat.11.long <- unique(dat.11[c("newADD","year","hour","volume")])
dat.long <- data.frame(rbind(dat.11.long, dat.13.long, dat.14.long))

summary.lmer.downtown.all1 <- 
  merge(summary.lmer.downtown.all1, dat.long, by = c("hour", "newADD", "year"), all = FALSE)
summary.lmer.downtown.all1$weekday.bin <- 1

# CONVERT MPH TO KPH
summary.lmer.downtown.all1$speed85 <- summary.lmer.downtown.all1$speed85*1.60934
summary.lmer.downtown.all1$night.speed <- summary.lmer.downtown.all1$night.speed*1.60934


# COUNT ADJUSTMENTS
summary.lmer.downtown.all1$count.adj <- 0

for (x in 9:11){
  summary.lmer.downtown.all1$month <- x
  summary.lmer.downtown.all1$count.adj <- summary.lmer.downtown.all1$count.adj + 4*predict(lm.11a, summary.lmer.downtown.all1)
}
summary.lmer.downtown.all1$count.adj <- summary.lmer.downtown.all1$count.adj/3

# NON-NEGATIVE DELAY
summary.lmer.downtown.all1$speed1 <- pmin(summary.lmer.downtown.all1$speed, summary.lmer.downtown.all1$speed85)
summary.lmer.downtown.all1$speed2 <- pmin(summary.lmer.downtown.all1$speed, summary.lmer.downtown.all1$night.speed)

summary.lmer.downtown.all1$vkt.count.adj <-
  summary.lmer.downtown.all1$count.adj*(summary.lmer.downtown.all1$Length_m/1000)
summary.lmer.downtown.all1$vkt.count.adj.speed <-
  summary.lmer.downtown.all1$vkt.count.adj/summary.lmer.downtown.all1$speed
summary.lmer.downtown.all1$vkt.count.adj.speed1 <-
  summary.lmer.downtown.all1$vkt.count.adj/summary.lmer.downtown.all1$speed1
summary.lmer.downtown.all1$vkt.count.adj.speed2 <-
  summary.lmer.downtown.all1$vkt.count.adj/summary.lmer.downtown.all1$speed2
summary.lmer.downtown.all1$vkt.count.adj.speed85 <-
  summary.lmer.downtown.all1$vkt.count.adj/summary.lmer.downtown.all1$speed85
summary.lmer.downtown.all1$vkt.count.adj.night.speed <-
  summary.lmer.downtown.all1$vkt.count.adj/summary.lmer.downtown.all1$night.speed

# INTEGRATING MTO ADJUSTMENTS FOR 2011 (NEEDS TO BE DONE FOR OTHER YEARS AS WELL)
summary.lmer.downtown.all1$CorridorUID1 <- summary.lmer.downtown.all1$CorridorUID
summary.lmer.downtown.all1$CorridorUID1[summary.lmer.downtown.all1$CorridorUID %in% mto_corridors] <- 88

summary.lmer.downtown.all1$weekday <- 5 # Thursday

summary.lmer.downtown.all1$count.adj.all <- 
  summary.lmer.downtown.all1$count.adj
summary.lmer.downtown.all1$count.adj.all[summary.lmer.downtown.all1$CorridorUID %in% mto_corridors] <- 
  predict(lm.b, summary.lmer.downtown.all1[summary.lmer.downtown.all1$CorridorUID %in% mto_corridors,])
summary.lmer.downtown.all1$count.adj.all[summary.lmer.downtown.all1$CorridorUID %in% mto_corridors & summary.lmer.downtown.all1$count.adj.all < 1] <- 
  0
summary.lmer.downtown.all1$count.adj.all[summary.lmer.downtown.all1$CorridorUID %in% oth_corridors] <- 
  predict(lm.c, summary.lmer.downtown.all1[summary.lmer.downtown.all1$CorridorUID %in% oth_corridors,])


#######################################
# CORRIDOR ANALYSES
#######################################

dat.temp<-subset(dat.14, subset = CorridorUID > 0 & weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
corridor.hourly.perf.14 <- tti_summary(dat.temp, c("CorridorUID","hour"), 5, 12)


dat.temp<-subset(dat.14, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.sep.nov <- tti_summary(dat.temp, c("CorridorUID","hour"), 5, 3)


dat.temp<-subset(dat.13, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.13.sep.nov <- tti_summary(dat.temp, c("CorridorUID","hour"), 5, 3)


dat.temp<-subset(dat.11, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.11.sep.nov <- tti_summary(dat.temp, c("CorridorUID","hour"), 5, 3)


dat.temp<-subset(dat.14, subset = CorridorUID > 0 & weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway == 2] <- 0
corridor.directional.seasonal.perf.14 <- tti_summary(dat.temp, c("C_UID","month"), 5, 1)


dat.temp<-subset(dat.14, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.14.sep.nov <- tti_summary(dat.temp, c("C_UID","hour"), 5, 3)


dat.temp<-subset(dat.13, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.13.sep.nov <- tti_summary(dat.temp, c("C_UID","hour"), 5, 3)


dat.temp<-subset(dat.11, subset = CorridorUID > 0 & weekday.bin == 1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2] <- 0
corridor.directional.hourly.perf.11.sep.nov <- tti_summary(dat.temp, c("C_UID","hour"), 5, 3)


dat.temp<-subset(dat.14, subset = weekday.bin ==1 )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.hourly.perf.14 <- tti_summary(dat.temp, c("hour","C_UID"), 5, 12)


##################################
# ARTERIAL CORRIDOR ADJUSTMENTS
##################################

# BELOW ARE SOME AD HOC ANALYSES WHICH NEED TO BE RUN TO EXTRACT QUEEN AND COLLEGE CORRECTLY,
# AND ALSO TO EXTRACT THE TWO-PLUS STREET CORRIDORS (E.G. ALBION/WILSON, ETC.)

########
# 2014 #
########

tmp_p_uids <- c(19.1, 19.2, 13.1, 13.2, 17.1, 17.2)
tmp_c_uids <- c(2.1, 2.2, 74.1, 74.2, 20.3, 20.4, 12.3, 12.4, 29.1, 29.2, 84.1, 84.2, 
                33.3, 33.4, 63.3, 63.4, 78.1, 78.2, 28.1, 28.2)

dat.14.0812 <- subset(dat.14, subset = (P_UID %in% tmp_p_uids | C_UID %in% tmp_c_uids))
dat.14.0812$Ponly_bin <- 0
dat.14.0812$noP_bin <- 0
dat.14.0812$Ponly_bin[dat.14.0812$P_UID %in% temp_p_uids] <- 1
dat.14.0812$noP_bin[dat.14.0812$Ponly_bin == 0] <- 1

dat.14.0812$LengthUID <- dat.14.0812$noP_bin * dat.14.0812$CorridorUID 
                            + dat.14.0812$Ponly_bin * floor(dat.14.0812$P_UID)

dat.14.0812$L_UID <- dat.14.0812$noP_bin * dat.14.0812$C_UID
                            + dat.14.0812$Ponly_bin * dat.14.0812$P_UID

dat.14.0812$LengthUID[dat.14.0812$LengthUID == 2 | dat.14.0812$LengthUID == 74] <- 2
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 20 | dat.14.0812$LengthUID == 12] <- 12
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 29 | dat.14.0812$LengthUID == 84] <- 29
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 33 | dat.14.0812$LengthUID == 63] <- 33
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 78 | dat.14.0812$LengthUID == 28] <- 28


dat.14.0812$L_UID[dat.14.0812$L_UID == 2.1 | dat.14.0812$L_UID == 74.1] <- 2.1
dat.14.0812$L_UID[dat.14.0812$L_UID == 20.3 | dat.14.0812$L_UID == 12.3] <- 12.3
dat.14.0812$L_UID[dat.14.0812$L_UID == 29.1 | dat.14.0812$L_UID == 84.1] <- 29.1
dat.14.0812$L_UID[dat.14.0812$L_UID == 33.3 | dat.14.0812$L_UID == 63.3] <- 33.3
dat.14.0812$L_UID[dat.14.0812$L_UID == 78.1 | dat.14.0812$L_UID == 28.1] <- 28.1

dat.14.0812$L_UID[dat.14.0812$L_UID == 2.2 | dat.14.0812$L_UID == 74.2] <- 2.2
dat.14.0812$L_UID[dat.14.0812$L_UID == 20.4 | dat.14.0812$L_UID == 12.4] <- 12.4
dat.14.0812$L_UID[dat.14.0812$L_UID == 29.2 | dat.14.0812$L_UID == 84.2] <- 29.2
dat.14.0812$L_UID[dat.14.0812$L_UID == 33.4 | dat.14.0812$L_UID == 63.4] <- 33.4
dat.14.0812$L_UID[dat.14.0812$L_UID == 78.2 | dat.14.0812$L_UID == 28.2] <- 28.2


########
# 2013 #
########

tmp_p_uids <- c(19.1, 19.2, 13.1, 13.2, 17.1, 17.2)
tmp_c_uids <- c(2.1, 2.2, 74.1, 74.2, 20.3, 20.4, 12.3, 12.4, 29.1, 29.2, 84.1, 84.2, 
                33.3, 33.4, 63.3, 63.4, 78.1, 78.2, 28.1, 28.2)

dat.13.0812 <- subset(dat.13, subset = (P_UID %in% tmp_p_uids | C_UID %in% tmp_c_uids))
dat.13.0812$Ponly_bin <- 0
dat.13.0812$noP_bin <- 0
dat.13.0812$Ponly_bin[dat.13.0812$P_UID %in% temp_p_uids] <- 1
dat.13.0812$noP_bin[dat.13.0812$Ponly_bin == 0] <- 1

dat.13.0812$LengthUID <- dat.13.0812$noP_bin * dat.13.0812$CorridorUID 
+ dat.13.0812$Ponly_bin * floor(dat.13.0812$P_UID)

dat.13.0812$L_UID <- dat.13.0812$noP_bin * dat.13.0812$C_UID
+ dat.13.0812$Ponly_bin * dat.13.0812$P_UID

dat.13.0812$LengthUID[dat.13.0812$LengthUID == 2 | dat.13.0812$LengthUID == 74] <- 2
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 20 | dat.13.0812$LengthUID == 12] <- 12
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 29 | dat.13.0812$LengthUID == 84] <- 29
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 33 | dat.13.0812$LengthUID == 63] <- 33
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 78 | dat.13.0812$LengthUID == 28] <- 28


dat.13.0812$L_UID[dat.13.0812$L_UID == 2.1 | dat.13.0812$L_UID == 74.1] <- 2.1
dat.13.0812$L_UID[dat.13.0812$L_UID == 20.3 | dat.13.0812$L_UID == 12.3] <- 12.3
dat.13.0812$L_UID[dat.13.0812$L_UID == 29.1 | dat.13.0812$L_UID == 84.1] <- 29.1
dat.13.0812$L_UID[dat.13.0812$L_UID == 33.3 | dat.13.0812$L_UID == 63.3] <- 33.3
dat.13.0812$L_UID[dat.13.0812$L_UID == 78.1 | dat.13.0812$L_UID == 28.1] <- 28.1

dat.13.0812$L_UID[dat.13.0812$L_UID == 2.2 | dat.13.0812$L_UID == 74.2] <- 2.2
dat.13.0812$L_UID[dat.13.0812$L_UID == 20.4 | dat.13.0812$L_UID == 12.4] <- 12.4
dat.13.0812$L_UID[dat.13.0812$L_UID == 29.2 | dat.13.0812$L_UID == 84.2] <- 29.2
dat.13.0812$L_UID[dat.13.0812$L_UID == 33.4 | dat.13.0812$L_UID == 63.4] <- 33.4
dat.13.0812$L_UID[dat.13.0812$L_UID == 78.2 | dat.13.0812$L_UID == 28.2] <- 28.2


########
# 2011 #
########

tmp_p_uids <- c(19.1, 19.2, 13.1, 13.2, 17.1, 17.2)
tmp_c_uids <- c(2.1, 2.2, 74.1, 74.2, 20.3, 20.4, 12.3, 12.4, 29.1, 29.2, 84.1, 84.2, 
                33.3, 33.4, 63.3, 63.4, 78.1, 78.2, 28.1, 28.2)

dat.11.0812 <- subset(dat.11, subset = (P_UID %in% tmp_p_uids | C_UID %in% tmp_c_uids))
dat.11.0812$Ponly_bin <- 0
dat.11.0812$noP_bin <- 0
dat.11.0812$Ponly_bin[dat.11.0812$P_UID %in% temp_p_uids] <- 1
dat.11.0812$noP_bin[dat.11.0812$Ponly_bin == 0] <- 1

dat.11.0812$LengthUID <- dat.11.0812$noP_bin * dat.11.0812$CorridorUID 
+ dat.11.0812$Ponly_bin * floor(dat.11.0812$P_UID)

dat.11.0812$L_UID <- dat.11.0812$noP_bin * dat.11.0812$C_UID
+ dat.11.0812$Ponly_bin * dat.11.0812$P_UID

dat.11.0812$LengthUID[dat.11.0812$LengthUID == 2 | dat.11.0812$LengthUID == 74] <- 2
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 20 | dat.11.0812$LengthUID == 12] <- 12
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 29 | dat.11.0812$LengthUID == 84] <- 29
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 33 | dat.11.0812$LengthUID == 63] <- 33
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 78 | dat.11.0812$LengthUID == 28] <- 28


dat.11.0812$L_UID[dat.11.0812$L_UID == 2.1 | dat.11.0812$L_UID == 74.1] <- 2.1
dat.11.0812$L_UID[dat.11.0812$L_UID == 20.3 | dat.11.0812$L_UID == 12.3] <- 12.3
dat.11.0812$L_UID[dat.11.0812$L_UID == 29.1 | dat.11.0812$L_UID == 84.1] <- 29.1
dat.11.0812$L_UID[dat.11.0812$L_UID == 33.3 | dat.11.0812$L_UID == 63.3] <- 33.3
dat.11.0812$L_UID[dat.11.0812$L_UID == 78.1 | dat.11.0812$L_UID == 28.1] <- 28.1

dat.11.0812$L_UID[dat.11.0812$L_UID == 2.2 | dat.11.0812$L_UID == 74.2] <- 2.2
dat.11.0812$L_UID[dat.11.0812$L_UID == 20.4 | dat.11.0812$L_UID == 12.4] <- 12.4
dat.11.0812$L_UID[dat.11.0812$L_UID == 29.2 | dat.11.0812$L_UID == 84.2] <- 29.2
dat.11.0812$L_UID[dat.11.0812$L_UID == 33.4 | dat.11.0812$L_UID == 63.4] <- 33.4
dat.11.0812$L_UID[dat.11.0812$L_UID == 78.2 | dat.11.0812$L_UID == 28.2] <- 28.2

#############
# SUMMARIES #
#############

dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.0812 <- tti_summary(dat.temp, c("LengthUID", "hour"), 5, 12)


dat.temp<-subset(dat.14.0812, subset = weekday.bin == 1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.sep.nov.0812 <- tti_summary(dat.temp, c("LengthUID", "hour"), 5, 3)

                                                    
dat.temp<-subset(dat.13.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.13.sep.nov.0812 <- tti_summary(dat.temp, c("LengthUID", "hour"), 5, 3)


dat.temp<-subset(dat.11.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.11.sep.nov.0812 <- tti_summary(dat.temp, c("LengthUID", "hour"), 5, 3)


dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.seasonal.perf.14.0812 <- tti_summary(dat.temp, c("L_UID", "month"), 5, 1)


dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.14.sep.nov.0812 <- tti_summary(dat.temp, c("L_UID", "hour"), 5, 3)


dat.temp<-subset(dat.13.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.13.sep.nov.0812 <- tti_summary(dat.temp, c("L_UID", "hour"), 5, 3)


dat.temp<-subset(dat.11.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.11.sep.nov.0812 <- tti_summary(dat.temp, c("L_UID", "hour"), 5, 3)


dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1 )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.hourly.perf.14.0812 <- tti_summary(dat.temp, c("hour","L_UID"), 5, 12)


dat.temp<-subset(dat.14.0812, subset = weekday.bin !="NA" )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.weekday.perf.14.0812 <- tti_summary(dat.temp, c("hour","weekday","L_UID"), 1, 12)

##############################
# GARDINER EXPRESSWAY MODELS
############################## 

dat.gardiner.wb <- subset(dat.all, subset = C_UID == 29.2 | C_UID == 84.2)
lmer.gardiner.wb <- lmer( speed.wtd ~ 1
                          + (1|tmc:hour:weekday)
                          + (1|hour:month:year)
                          + (1|month)
                          + (1|year)
                          + (1|hour:weekday.bin),
                          data = dat.gardiner.wb,
                          weights = weight.adj.all,
                          subset = (month<12 & month >8))
tidy(lmer.gardiner.wb)
fixef(lmer.gardiner.wb)
ranef(lmer.gardiner.wb)

dat.gardiner.wb$tti.85<-dat.gardiner.wb$speed85/dat.gardiner.wb$speed.wtd1
lmer.gardiner.wb.tti <- lmer( tti.85 ~ 1
                              + (1|tmc:hour:weekday)
                              + (1|hour:month:year)
                              + (1|month)
                              + (1|year)
                              + (1|hour:weekday.bin), 
                              data = dat.gardiner.wb,
                              weights = weight.adj.all,
                              subset = (month<12 & month >8))
tidy(lmer.gardiner.wb.tti)
ranef(lmer.gardiner.wb.tti)
fixef(lmer.gardiner.wb.tti)



dat.gardiner.eb<-subset(dat.all, subset = C_UID == 29.1 | C_UID == 84.1)
lmer.gardiner.eb <- lmer( speed.wtd ~ 1
                          + (1|tmc:hour:weekday)
                          + (1|hour:month:year)
                          + (1|month)
                          + (1|year)
                          + (1|hour:weekday.bin),
                          data = dat.gardiner.eb,
                          weights = weight.adj.all,
                          subset = (month<12 & month >8))
tidy(lmer.gardiner.eb)
fixef(lmer.gardiner.eb)
ranef(lmer.gardiner.eb)

dat.gardiner.eb$tti.85<-dat.gardiner.eb$speed85/dat.gardiner.eb$speed.wtd1
lmer.gardiner.eb.tti <- lmer( tti.85 ~ 1
                              + (1|tmc:hour:weekday)
                              + (1|hour:month:year)
                              + (1|month)
                              + (1|year)
                              + (1|hour:weekday.bin), 
                              data = dat.gardiner.eb,
                              weights = weight.adj.all,
                              subset = (month<12 & month >8))
tidy(lmer.gardiner.eb.tti)
ranef(lmer.gardiner.eb.tti)
fixef(lmer.gardiner.eb.tti)