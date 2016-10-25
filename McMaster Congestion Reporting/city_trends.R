library(plyr)
library(data.table)
library(ggplot2)
library(reshape2)
library(broom)
library(RPostgreSQL)

analysis_year = 2014

##############################################
# IMPORT RAW DATA FROM POSTGRESQL
##############################################

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

fileLoc = "K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash"

impute<-function(a, a.imute){
  ifelse(is.na(a),a.impute,a)
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


##############################################
# IMPORT VARIOUS REFERENCE TABLES
##############################################

curr_dir <- "in/"

# LOOKUP TABLE, linking newADD to TMC
dat.tmc.newADD <-
    read.table(file = paste(curr_dir,"trafficInrix_Join_Jan27b_tmc_newADD.csv",sep=""), 
               sep = ",", 
               col.names = c("newADD", "tmc"),
               header = TRUE)

# REFERENCE TABLE, newADD properties
dat.final.net <- 
    read.table(file = paste(curr_dir,"final_net.csv",sep=""), 
               sep = ",", 
               header = TRUE)

# FREE FLOW REFERENCE SPEEDS (by TMC), night.speed and 85.speed
dat.speed85 <-
    read.table(file = paste(curr_dir,"INRIX_speed85.txt",sep=""), 
             header = TRUE)

torNetwork_UIDs_feb10 <- 
    read.table(paste(curr_dir,"torNetwork_UIDs_feb10.csv",sep=""), 
               sep = ",", 
               header = TRUE)
torNetwork_UIDs_feb10 <- unique(torNetwork_UIDs_feb10)

# NOTE THAT THE 2014 TRAFFIC VOLUMES ARE BASED ON A LINEAR TREND INTERPRETATION BETWEEN 2011 AND 2016 TRAFFIC RESULTS.
# THE UNITS HERE ARE VOLUMES PER HOUR.  

########
# 2014 #
########
dat.14a <-
    read.table(paste(curr_dir, "final_net2014.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.14a <-
    melt(dat.14a, id="newADD")
colnames(dat.14a) <- c("newADD", "time.factor1", "volume1")

time.factor <- as.numeric(dat.14a$time.factor1)
volume      <- as.numeric(dat.14a$volume1)
newADD      <- as.numeric(dat.14a$newADD)
dat.14a     <- data.frame(cbind(newADD, time.factor, volume))
rm(time.factor, volume, newADD)

########
# 2013 #
########
dat.13a <-
  read.table(paste(curr_dir, "final_net2013.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.13a <-
  melt(dat.13a, id="newADD")
colnames(dat.13a) <- c("newADD", "time.factor1", "volume1")

time.factor <- as.numeric(dat.13a$time.factor1)
volume      <- as.numeric(dat.13a$volume1)
newADD      <- as.numeric(dat.13a$newADD)
dat.13a     <- data.frame(cbind(newADD, time.factor, volume))
rm(time.factor, volume, newADD)

########
# 2011 #
########
dat.11a <-
  read.table(paste(curr_dir, "final_net2011.csv", sep=""),
             sep = ",",
             header = TRUE)
dat.11a <-
  melt(dat.11a, id="newADD")
colnames(dat.11a) <- c("newADD", "time.factor1", "volume1")

time.factor <- as.numeric(dat.11a$time.factor1)
volume      <- as.numeric(dat.11a$volume1)
newADD      <- as.numeric(dat.11a$newADD)
dat.11a     <- data.frame(cbind(newADD, time.factor, volume))
rm(time.factor, volume, newADD)


# Clean up Column Names
colnames(dat.14a)[colnames(dat.14a)=="time.factor"] <- "hour"
colnames(dat.13a)[colnames(dat.13a)=="time.factor"] <- "hour"
colnames(dat.11a)[colnames(dat.11a)=="time.factor"] <- "hour"
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


##############################################
# LOAD (OR CALCULATE) VOLUME ADJUSTMENT MODEL
##############################################

load(paste(curr_dir, "vol_adj.rda",sep=""))


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

##############################################
# MTO ADJUSTMENTS
##############################################

dat.mto.vol$date <- as.Date(dat.mto.vol$date)
dat.mto.vol$month <- as.numeric(format(dat.mto.vol$date, format = "%m"))
dat.mto.vol$year <- 2013
dat.mto.vol$year[dat.mto.vol$mto.location.id > 28] <- 2014

save.image("CITY_TRENDS.RData")

#### START HERE ####




dat.13a<-dat.13[,c("weekday", "month", "hour", "newADD", "count.adj", "CorridorUID")]
year<-rep(2013, length=dim(dat.13a)[1])
dat.13a<-data.frame(cbind(dat.13a, year))

dat.14a<-dat.14[,c("weekday", "month", "hour", "newADD", "count.adj", "CorridorUID")]
year<-rep(2014, length=dim(dat.14a)[1])
dat.14a<-data.frame(cbind(dat.14a, year))

dat.13a.14a<-data.frame(rbind(dat.13a, dat.14a))
rm(dat.13a, dat.14a)

dat.13a.14a<-merge(dat.mto.vol, dat.13a.14a, by = c("weekday", "month", "hour", "newADD", "year"), all = FALSE)
colnames(dat.13a.14a)[colnames(dat.13a.14a)=="CorridorUID"]<-"CorridorUID1"

lm.b<-lm(mto.volume~as.factor(CorridorUID1)+as.factor(weekday!=1 & weekday !=7)
           +count.adj:as.factor(weekday!=1 & weekday !=7 )+count.adj:as.factor(CorridorUID1), data = dat.13a.14a)
summary(lm.b)
plot(fitted(lm.b), resid(lm.b))
hist(fitted(lm.b))

lm.c<-lm(mto.volume~as.factor(weekday!=1 & weekday !=7)
         +count.adj:as.factor(weekday!=1 & weekday !=7 ), data = dat.13a.14a)
summary(lm.c)
plot(fitted(lm.c), resid(lm.c))

summary.lm.b<-tidy(lm.b)
summary.lm.c<-tidy(lm.c)
setwd("Y:/modeling/out/seasonal_adjustments/") #sets the working directory.
#write.table(summary.lm.b, file = "summary.lm.b.txt")
#write.table(summary.lm.c, file = "summary.lm.c.txt")
rm(summary.lm.b, summary.lm.c)

#INTEGRATING MTO ADJUSTMENTS FOR 2014.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####
CorridorUID1<-dat.14$CorridorUID
CorridorUID1[CorridorUID1!=81 &CorridorUID1!=82&CorridorUID1!=80&CorridorUID1!=88
             &CorridorUID1!=85&CorridorUID1!=83]<-88
dat.14<-data.frame(cbind(dat.14, CorridorUID1))
rm(CorridorUID1)
count.adj1<-predict(lm.b, dat.14)
count.adj1[count.adj1<1]<-0
count.adj2<-predict(lm.c, dat.14)

bin.1<-rep(0, length = length(count.adj1))
bin.1[dat.14$CorridorUID==80 | dat.14$CorridorUID==81 | dat.14$CorridorUID==82 | 
        dat.14$CorridorUID==83 | dat.14$CorridorUID==85 | dat.14$CorridorUID==88]<-1
bin.2<-rep(0, length = length(count.adj1))
bin.2[dat.14$CorridorUID==87 | dat.14$CorridorUID==89 ]<-1
bin.3<-rep(0, length= length(count.adj1))
bin.3[bin.1==0&bin.2==0]<-1
count.adj.all<-dat.14$count.adj*bin.3+count.adj1*bin.1+count.adj2*bin.2
dat.14<-data.frame(cbind(dat.14, count.adj.all))
names(dat.14)
rm(dat.14a)
rm(bin.1, bin.2, bin.3, count.adj.all, count.adj1, count.adj2)



#INTEGRATING MTO ADJUSTMENTS FOR 2013.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####
CorridorUID1<-dat.13$CorridorUID
CorridorUID1[CorridorUID1!=81 &CorridorUID1!=82&CorridorUID1!=80&CorridorUID1!=88
             &CorridorUID1!=85&CorridorUID1!=83]<-88
dat.13<-data.frame(cbind(dat.13, CorridorUID1))
rm(CorridorUID1)
count.adj1<-predict(lm.b, dat.13)
count.adj1[count.adj1<1]<-0
count.adj2<-predict(lm.c, dat.13)


bin.1<-rep(0, length = length(count.adj1))
bin.1[dat.13$CorridorUID==80 | dat.13$CorridorUID==81 | dat.13$CorridorUID==82 | 
        dat.13$CorridorUID==83 | dat.13$CorridorUID==85 | dat.13$CorridorUID==88]<-1
bin.2<-rep(0, length = length(count.adj1))
bin.2[dat.13$CorridorUID==87 | dat.13$CorridorUID==89 ]<-1
bin.3<-rep(0, length= length(count.adj1))
bin.3[bin.1==0&bin.2==0]<-1
count.adj.all<-dat.13$count.adj*bin.3+count.adj1*bin.1+count.adj2*bin.2
dat.13<-data.frame(cbind(dat.13, count.adj.all))
names(dat.13)
rm(bin.1, bin.2, bin.3, count.adj.all, count.adj1, count.adj2)

#INTEGRATING MTO ADJUSTMENTS FOR 2011.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####
CorridorUID1<-dat.11$CorridorUID
CorridorUID1[CorridorUID1!=81 &CorridorUID1!=82&CorridorUID1!=80&CorridorUID1!=88
             &CorridorUID1!=85&CorridorUID1!=83]<-88
dat.11<-data.frame(cbind(dat.11, CorridorUID1))
rm(CorridorUID1)
count.adj1<-predict(lm.b, dat.11)
count.adj1[count.adj1<1]<-0
count.adj2<-predict(lm.c, dat.11)

bin.1<-rep(0, length = length(count.adj1))
bin.1[dat.11$CorridorUID==80 | dat.11$CorridorUID==81 | dat.11$CorridorUID==82 | 
        dat.11$CorridorUID==83 | dat.11$CorridorUID==85 | dat.11$CorridorUID==88]<-1
bin.2<-rep(0, length = length(count.adj1))
bin.2[dat.11$CorridorUID==87 | dat.11$CorridorUID==89 ]<-1
bin.3<-rep(0, length= length(count.adj1))
bin.3[bin.1==0&bin.2==0]<-1
count.adj.all<-dat.11$count.adj*bin.3+count.adj1*bin.1+count.adj2*bin.2
dat.11<-data.frame(cbind(dat.11, count.adj.all))
names(dat.11)
rm(dat.11a)
rm(bin.1, bin.2, bin.3, count.adj.all, count.adj1, count.adj2)



#ADDING WEIGHTS

weight.vol<-(dat.14$volume*dat.14$Length_m)/mean(dat.14$volume*dat.14$Length_m)
weight.adj<-(dat.14$count.adj*dat.14$Length_m)/mean(dat.14$count.adj*dat.14$Length_m)
weight.adj.all<-(dat.14$count.adj.all*dat.14$Length_m)/mean(dat.14$count.adj.all*dat.14$Length_m)
dat.14<-data.frame(cbind(dat.14, weight.vol, weight.adj, weight.adj.all))
rm(weight.vol, weight.adj, weight.adj.all)



#MODELING
rm(dat.11.long, dat.13.long, dat.14.long, dat.final.net, dat.mto.vol, dat.speed85, dat.tmc.newADD, level1, summary.lm.11, summary.lm.11a, 
   torNetwork_UIDs_feb10, year)

lmer.11b<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.11, weights = weight.adj)
lmer.13b<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.13, weights = weight.adj)
lmer.14b<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.14, weights = weight.adj)

summary(lmer.14b)
fixef(lmer.14b)
ranef(lmer.14b)$month
ranef(lmer.14b)$weekday
ranef(lmer.14b)$tmc
ranef(lmer.14b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.14b<-tidy(lmer.14b)
write.table(summary.lmer.14b, file = "summary.lmer.14b.txt")
write(fixef(lmer.14b), file = "fixef.lmer.14b.txt")
ranef.lmer.14b<-ranef(lmer.14b)$day.continuous
write.table(ranef.lmer.14b, file = "ranef.lmer.14b.txt")
rm(ranef.lmer.14b, summary.lmer.14b)
#rm(lmer.14b)

summary(lmer.13b)
fixef(lmer.13b)
ranef(lmer.13b)$month
ranef(lmer.13b)$weekday
ranef(lmer.13b)$tmc
ranef(lmer.13b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.13b<-tidy(lmer.13b)
write.table(summary.lmer.13b, file = "summary.lmer.13b.txt")
write(fixef(lmer.13b), file = "fixef.lmer.13b.txt")
ranef.lmer.13b<-ranef(lmer.13b)$day.continuous
write.table(ranef.lmer.13b, file = "ranef.lmer.13b.txt")
rm(ranef.lmer.13b, summary.lmer.13b)
#rm(lmer.13b)

summary(lmer.11b)
fixef(lmer.11b)
ranef(lmer.11b)$month
ranef(lmer.11b)$weekday
ranef(lmer.11b)$tmc
ranef(lmer.11b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.11b<-tidy(lmer.11b)
write.table(summary.lmer.11b, file = "summary.lmer.11b.txt")
write(fixef(lmer.11b), file = "fixef.lmer.11b.txt")
ranef.lmer.11b<-ranef(lmer.11b)$day.continuous
write.table(ranef.lmer.11b, file = "ranef.lmer.11b.txt")
rm(ranef.lmer.11b, summary.lmer.11b)
#rm(lmer.11b)





#ADJUSTED BY weight.adj.ALL
lmer.11c<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.11, weights = weight.adj.all)
lmer.13c<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.13, weights = weight.adj.all)
lmer.14c<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.14, weights = weight.adj.all)
lmer.14d<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month) + (1|hour), 
               data = dat.14, weights = weight.adj.all, subset = weekday.bin ==1)
summary(lmer.14d)
ranef(lmer.14d)$hour
ranef(lmer.14d)$month

summary(lmer.14c)
fixef(lmer.14c)
ranef(lmer.14c)$month
ranef(lmer.14c)$weekday
ranef(lmer.14c)$tmc
ranef(lmer.14c)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.14c<-tidy(lmer.14c)
write.table(summary.lmer.14c, file = "summary.lmer.14c.txt")
write(fixef(lmer.14c), file = "fixef.lmer.14c.txt")
ranef.lmer.14c<-ranef(lmer.14c)$day.continuous
write.table(ranef.lmer.14c, file = "ranef.lmer.14c.txt")
rm(ranef.lmer.14c, summary.lmer.14c)
#rm(lmer.14c)

summary(lmer.13c)
fixef(lmer.13c)
ranef(lmer.13c)$month
ranef(lmer.13c)$weekday
ranef(lmer.13c)$tmc
ranef(lmer.13c)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.13c<-tidy(lmer.13c)
write.table(summary.lmer.13c, file = "summary.lmer.13c.txt")
write(fixef(lmer.13c), file = "fixef.lmer.13c.txt")
ranef.lmer.13c<-ranef(lmer.13c)$day.continuous
write.table(ranef.lmer.13c, file = "ranef.lmer.13c.txt")
rm(ranef.lmer.13c, summary.lmer.13c)
#rm(lmer.13c)

summary(lmer.11c)
fixef(lmer.11c)
ranef(lmer.11c)$month
ranef(lmer.11c)$weekday
ranef(lmer.11c)$tmc
ranef(lmer.11c)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.11c<-tidy(lmer.11c)
write.table(summary.lmer.11c, file = "summary.lmer.11c.txt")
write(fixef(lmer.11c), file = "fixef.lmer.11c.txt")
ranef.lmer.11c<-ranef(lmer.11c)$day.continuous
write.table(ranef.lmer.11c, file = "ranef.lmer.11c.txt")
rm(ranef.lmer.11c, summary.lmer.11c)
#rm(lmer.11c)





year<-rep(2011, length = dim(dat.11)[1])
dat.11<-data.frame(cbind(dat.11, year))
year<-rep(2013, length = dim(dat.13)[1])
dat.13<-data.frame(cbind(dat.13, year))
year<-rep(2014, length = dim(dat.14)[1])
dat.14<-data.frame(cbind(dat.14, year))
rm(year)
dat.all<-data.frame(rbind(dat.11, dat.13, dat.14))


lmer.all<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month)+(1|year), 
               data = dat.all, weights = weight.adj.all, subset = Freeway ==1)
summary(lmer.all)
fixef(lmer.all)
ranef(lmer.all)$month
ranef(lmer.all)$weekday
ranef(lmer.all)$tmc
ranef(lmer.all)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.all<-tidy(lmer.all)
write.table(summary.lmer.all, file = "summary.lmer.all.txt")
write(fixef(lmer.all), file = "fixef.lmer.all.txt")
ranef.lmer.all<-ranef(lmer.all)$day.continuous
write.table(ranef.lmer.all, file = "ranef.lmer.all.txt")
rm(ranef.lmer.all, summary.lmer.all)
#rm(lmer.all)

lmer.fre.year.hourly<-lmer(speed.wtd~1+(1|tmc:hour)+ (1|month)+(1|year:hour), 
               data = dat.all, weights = weight.adj.all, subset = (Freeway ==1 & weekday.bin ==1))
summary(lmer.fre.year.hourly)
fixef(lmer.fre.year.hourly)
ranef(lmer.fre.year.hourly)$month
ranef(lmer.fre.year.hourly)$weekday
ranef(lmer.fre.year.hourly)$tmc
ranef(lmer.fre.year.hourly)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.fre.year.hourly<-tidy(lmer.fre.year.hourly)
write.table(summary.lmer.fre.year.hourly, file = "summary.lmer.fre.year.hourly.txt")
write(fixef(lmer.fre.year.hourly), file = "fixef.lmer.fre.year.hourly.txt")
ranef.lmer.fre.year.hourly<-ranef(lmer.fre.year.hourly)$day.continuous
write.table(ranef.lmer.fre.year.hourly, file = "ranef.lmer.fre.year.hourly.txt")
rm(ranef.lmer.fre.year.hourly, summary.lmer.fre.year.hourly)
#rm(lmer.fre.year.hourly)


lmer.art.year.hourly<-lmer(speed.wtd~1+(1|tmc:hour)+ (1|month)+(1|year:hour), 
                           data = dat.all, weights = weight.adj.all, subset = (Freeway !=1 & weekday.bin ==1))
summary(lmer.art.year.hourly)
fixef(lmer.art.year.hourly)
ranef(lmer.art.year.hourly)$month
ranef(lmer.art.year.hourly)$weekday
ranef(lmer.art.year.hourly)$tmc
ranef(lmer.art.year.hourly)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.art.year.hourly<-tidy(lmer.art.year.hourly)
write.table(summary.lmer.art.year.hourly, file = "summary.lmer.art.year.hourly.txt")
write(fixef(lmer.art.year.hourly), file = "fixef.lmer.art.year.hourly.txt")
ranef.lmer.art.year.hourly<-ranef(lmer.art.year.hourly)$day.continuous
write.table(ranef.lmer.art.year.hourly, file = "ranef.lmer.art.year.hourly.txt")
rm(ranef.lmer.art.year.hourly, summary.lmer.art.year.hourly)
#rm(lmer.art.year.hourly)


save.image("Y:/modeling/rcode/CITY_TRENDS_06-11-2015.RData")





#EXTRACTING THE RESULTS FOR FURTHER PROCESSING:
summary.lmer.14b<-tidy(lmer.14b)
n.12<-(dim(summary.lmer.14b)[1]-12)
n<-dim(summary.lmer.14b)[1]

summary.lmer.14b.month<-summary.lmer.14b[(n.12+1):n,]
summary.lmer.14b<-summary.lmer.14b[1:n.12,]
dim(summary.lmer.14b)
names(summary.lmer.14b)
level1<-colsplit(summary.lmer.14b$level,":", c("tmc", "hour", "weekday"))
summary.lmer.14b<-data.frame(cbind(level1, summary.lmer.14b$value))
names(summary.lmer.14b)<-c("tmc", "hour", "weekday", "speed")
summary.lmer.14b.month<-data.frame(summary.lmer.14b.month[,c("level", "value")])
names(summary.lmer.14b.month)<-c("month", "speed")
summary.lmer.14b.month
summary.lmer.14b.month$speed<-summary.lmer.14b.month$speed*1.60934
summary.lmer.14b$speed<-summary.lmer.14b$speed*1.60934

summary.lmer.14b<-merge(summary.lmer.14b, dat.tmc.newADD, by = "tmc", all = FALSE)
summary.lmer.14b$newADD<-as.numeric(summary.lmer.14b$newADD)
hist(summary.lmer.14b$newADD)
summary.lmer.14b<-merge(summary.lmer.14b, dat.speed85, by = "tmc" , all = FALSE)
summary.lmer.14b<-merge(summary.lmer.14b, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.final.net1<-dat.final.net[,c("newADD", "Freeway", "Length_m")]
summary.lmer.14b<-merge(summary.lmer.14b, dat.final.net1, by = "newADD", all = FALSE)
rm(dat.final.net1)
summary.lmer.14b<-merge(summary.lmer.14b, dat.14.long, by = c("hour", "newADD"), all = FALSE)

weekday.bin<-rep(1,length=dim(summary.lmer.14b)[1])
weekday.bin[summary.lmer.14b$weekday==1| summary.lmer.14b$weekday==7]<-0
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, weekday.bin))
rm(weekday.bin)
dim(summary.lmer.14b)

#all is adjusted to kph
summary.lmer.14b$speed85<-summary.lmer.14b$speed85*1.60934
summary.lmer.14b$night.speed<-summary.lmer.14b$night.speed*1.60934


#COUNT ADJUSTMENTS ARE CALCULATED HERE.  
month<-rep(0, length = dim(summary.lmer.14b)[1])
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, month))
rm(month)

summary.lmer.14b$month<-1
count.adj.1<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-2
count.adj.2<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-3
count.adj.3<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-4
count.adj.4<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-5
count.adj.5<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-6
count.adj.6<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-7
count.adj.7<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-8
count.adj.8<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-9
count.adj.9<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-10
count.adj.10<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-11
count.adj.11<-4*predict(lm.11a, summary.lmer.14b)
summary.lmer.14b$month<-12
count.adj.12<-4*predict(lm.11a, summary.lmer.14b)
count.adj<-(count.adj.1+ count.adj.2+ count.adj.3+ count.adj.4+
               count.adj.5+ count.adj.6+ count.adj.7+ count.adj.8+
               count.adj.9+ count.adj.10+ count.adj.11+ count.adj.12)/12
hist(count.adj)
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, count.adj))
rm(count.adj, count.adj.1, count.adj.2, count.adj.3, count.adj.4,
   count.adj.5, count.adj.6, count.adj.7, count.adj.8,
   count.adj.9, count.adj.10, count.adj.11, count.adj.12)

#PROCESSING SO THAT DELAY IS NEVER NEGATIVE
speed1<-summary.lmer.14b$speed
speed1<-pmin(speed1, summary.lmer.14b$speed85)
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, speed1))
rm(speed1)

speed2<-summary.lmer.14b$speed
speed2<-pmin(speed2, summary.lmer.14b$night.speed)
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, speed2))
rm(speed2)

vkt.count.adj<-summary.lmer.14b$count.adj*(summary.lmer.14b$Length_m/1000)
vkt.count.adj.speed<-vkt.count.adj/summary.lmer.14b$speed
vkt.count.adj.speed1<-vkt.count.adj/summary.lmer.14b$speed1
vkt.count.adj.speed2<-vkt.count.adj/summary.lmer.14b$speed2
vkt.count.adj.speed85<-vkt.count.adj/summary.lmer.14b$speed85
vkt.count.adj.night.speed<-vkt.count.adj/summary.lmer.14b$night.speed

summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, vkt.count.adj, vkt.count.adj.speed, vkt.count.adj.speed1, 
                                   vkt.count.adj.speed2, vkt.count.adj.speed85,
                                   vkt.count.adj.night.speed))
rm(vkt.count.adj, vkt.count.adj.speed, vkt.count.adj.speed1, 
   vkt.count.adj.speed2, vkt.count.adj.speed85,
   vkt.count.adj.night.speed)
rm(n, n.11, n.12, year)
#INTEGRATING MTO ADJUSTMENTS FOR 2011.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####


CorridorUID1<-summary.lmer.14b$CorridorUID
CorridorUID1[CorridorUID1!=81 &CorridorUID1!=82&CorridorUID1!=80&CorridorUID1!=88
             &CorridorUID1!=85&CorridorUID1!=83]<-88
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, CorridorUID1))
rm(CorridorUID1)
count.adj1<-predict(lm.b, summary.lmer.14b)
count.adj1[count.adj1<1]<-0
count.adj2<-predict(lm.c, summary.lmer.14b)

bin.1<-rep(0, length = length(count.adj1))
bin.1[summary.lmer.14b$CorridorUID==80 | summary.lmer.14b$CorridorUID==81 | summary.lmer.14b$CorridorUID==82 | 
        summary.lmer.14b$CorridorUID==83 | summary.lmer.14b$CorridorUID==85 | summary.lmer.14b$CorridorUID==88]<-1
bin.2<-rep(0, length = length(count.adj1))
bin.2[summary.lmer.14b$CorridorUID==87 | summary.lmer.14b$CorridorUID==89 ]<-1
bin.3<-rep(0, length= length(count.adj1))
bin.3[bin.1==0&bin.2==0]<-1
count.adj.all<-summary.lmer.14b$count.adj*bin.3+count.adj1*bin.1+count.adj2*bin.2
summary.lmer.14b<-data.frame(cbind(summary.lmer.14b, count.adj.all))
names(summary.lmer.14b)
rm(summary.lmer.14ba)
rm(bin.1, bin.2, bin.3, count.adj.all, count.adj1, count.adj2)





setwd("Y:/modeling/out/city_trends/") #sets the working directory.
write.table(summary.lmer.14b, file = "summary.lmer.14b.proc.txt")

dat.temp<-subset(summary.lmer.14b, subset = (weekday.bin == 1& Freeway ==1))
out.lmer.14b.daily<-ddply(dat.temp, . (hour),
                                     summarise,
                                     vkt.k=sum(vkt.count.adj),
                                     delay.85.hrs=sum(vkt.count.adj.speed1-vkt.count.adj.speed85),
                                     delay.night.hrs=sum(vkt.count.adj.speed2-vkt.count.adj.night.speed),
                                     tti.85=(sum(vkt.count.adj.speed1)/sum(vkt.count.adj.speed85)),
                                     tti.night=(sum(vkt.count.adj.speed2)/sum(vkt.count.adj.night.speed)),
                                     speed=sum(speed*vkt.count.adj)/sum(vkt.count.adj)
                                     )
out.lmer.14b.daily
attach(out.lmer.14b.daily)
names(out.lmer.14b.daily)
plot(hour,speed)
plot(hour,tti.night)
plot(hour,vkt.k)
plot(hour,delay.night.hrs)

detach(out.lmer.14b.daily)







lmer.11e<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.11, weights = weight.adj)
lmer.13e<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.13, weights = weight.adj)
lmer.14e<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.14, weights = weight.adj)

summary(lmer.14b)
fixef(lmer.14b)
ranef(lmer.14b)$month
ranef(lmer.14b)$weekday
ranef(lmer.14b)$tmc
ranef(lmer.14b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.14b<-tidy(lmer.14b)
write.table(summary.lmer.14b, file = "summary.lmer.14b.txt")
write(fixef(lmer.14b), file = "fixef.lmer.14b.txt")
ranef.lmer.14b<-ranef(lmer.14b)$day.continuous
write.table(ranef.lmer.14b, file = "ranef.lmer.14b.txt")
rm(ranef.lmer.14b, summary.lmer.14b)
#rm(lmer.14b)

summary(lmer.13b)
fixef(lmer.13b)
ranef(lmer.13b)$month
ranef(lmer.13b)$weekday
ranef(lmer.13b)$tmc
ranef(lmer.13b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.13b<-tidy(lmer.13b)
write.table(summary.lmer.13b, file = "summary.lmer.13b.txt")
write(fixef(lmer.13b), file = "fixef.lmer.13b.txt")
ranef.lmer.13b<-ranef(lmer.13b)$day.continuous
write.table(ranef.lmer.13b, file = "ranef.lmer.13b.txt")
rm(ranef.lmer.13b, summary.lmer.13b)
#rm(lmer.13b)

summary(lmer.11b)
fixef(lmer.11b)
ranef(lmer.11b)$month
ranef(lmer.11b)$weekday
ranef(lmer.11b)$tmc
ranef(lmer.11b)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.11b<-tidy(lmer.11b)
write.table(summary.lmer.11b, file = "summary.lmer.11b.txt")
write(fixef(lmer.11b), file = "fixef.lmer.11b.txt")
ranef.lmer.11b<-ranef(lmer.11b)$day.continuous
write.table(ranef.lmer.11b, file = "ranef.lmer.11b.txt")
rm(ranef.lmer.11b, summary.lmer.11b)
#rm(lmer.11b)



































save.image("Y:/modeling/rcode/CITY_TRENDS_06-11-2015.RData")

#this is where the code is built.  

#THE FOLLOWING CODE CALCULATES THE TRAVEL TIME INDICES FOR EACH OF THE CORRIDORS

dat.temp<-subset(dat.14, subset = weekday.bin ==1 
             )
dat.temp$C_UID[dat.temp$C_UID==29.1]<-84.1
dat.temp$C_UID[dat.temp$C_UID==29.2]<-84.2
dat.temp$C_UID[dat.temp$C_UID==29.3]<-84.3
dat.temp$C_UID[dat.temp$C_UID==29.4]<-84.4
corridor.direct.hourly.perf.14<-ddply(dat.temp, . (hour, C_UID
                                          ),
                        summarise,
                        tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
                        tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
                        tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
                        tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
                        tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
                        tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
                        speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
                        speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
                        speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
                        delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
                        delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
                        delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
                        delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
                        delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
                        delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
                        vkt.volume=sum(volume*(Length_m/1000))/(5*12),
                        vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
                        vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)

corridor.direct.hourly.perf.14

setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(corridor.direct.hourly.perf.14, file = "corridor.direct.hourly.perf.14.txt")



dat.temp<-subset(dat.14, subset = weekday.bin ==1 )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.hourly.perf.14<-ddply(dat.temp, . (hour, CorridorUID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)

corridor.hourly.perf.14

setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(corridor.hourly.perf.14, file = "corridor.hourly.perf.14.txt")


attach(corridor.hourly.perf.14)
plot(hour, delay.night.volume)
plot(hour, delay.night.count.adj)
plot(hour, delay.night.count.adj.all)
plot(hour, vkt.volume)
plot(hour, vkt.count.adj)
plot(hour, vkt.count.adj.all)

plot((hour[CorridorUID==84 | CorridorUID==84]),(tti.85.volume[CorridorUID==84| CorridorUID==84]))
plot((hour[CorridorUID==84 | CorridorUID==84]),(speed.count.adj[CorridorUID==84| CorridorUID==84]))
plot((hour[CorridorUID==84 | CorridorUID==84]),(delay.85.count.adj[CorridorUID==84| CorridorUID==84]))
plot((vkt.count.adj[CorridorUID==84 | CorridorUID==84]),(delay.85.count.adj[CorridorUID==84| CorridorUID==84]))

detach(corridor.hourly.perf.14)


dat.temp<-subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
system.hourly.perf.14<-ddply(dat.temp, . (hour#, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(system.hourly.perf.14, file = "system.hourly.perf.14.txt")


dat.temp<-subset(dat.14, subset = weekday.bin == 0)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
system.hourly.perf.14.weekend<-ddply(dat.temp, . (hour#, CorridorUID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*2*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*2*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*2*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*2*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*2*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*2*12),
vkt.volume=sum(volume*(Length_m/1000))/(2*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(2*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(2*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(system.hourly.perf.14.weekend, file = "system.hourly.perf.14.weekend.txt")



dat.temp<-subset(dat.14, subset = weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.hourly.perf.14<-ddply(dat.temp, . (hour, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.hourly.perf.14, file = "fre.art.hourly.perf.14.txt")



dat.temp<-subset(dat.14, subset = weekday.bin == 0)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.hourly.perf.14.weekend<-ddply(dat.temp, . (hour, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*2*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*2*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*2*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*2*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*2*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*2*12),
vkt.volume=sum(volume*(Length_m/1000))/(2*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(2*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(2*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.hourly.perf.14.weekend, file = "fre.art.hourly.perf.14.weekend.txt")






dat.temp<-subset(dat.13, subset = weekday.bin == 1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.hourly.perf.13.sep.nov<-ddply(dat.temp, . (hour, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.hourly.perf.13.sep.nov, file = "fre.art.hourly.perf.13.sep.nov.txt")



dat.temp<-subset(dat.11, subset = weekday.bin == 1& (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.hourly.perf.11.sep.nov<-ddply(dat.temp, . (hour, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.hourly.perf.11.sep.nov, file = "fre.art.hourly.perf.11.sep.nov.txt")


dat.temp<-subset(dat.14, subset = weekday.bin == 1& (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.hourly.perf.14.sep.nov<-ddply(dat.temp, . (hour, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.hourly.perf.14.sep.nov, file = "fre.art.hourly.perf.14.sep.nov.txt")








dat.temp<-subset(dat.14, subset = hour ==17)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.daily.perf.14.5pm<-ddply(dat.temp, . (weekday, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*1*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12), #1 adjusts for number of day-types per week
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*1*12), # 12 adjusts for number of months per year
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.daily.perf.14.5pm, file = "fre.art.daily.perf.14.5pm.txt")

dat.temp<-(dat.14)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.daily.perf.14<-ddply(dat.temp, . (weekday, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*1*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12), #1 adjusts for number of day-types per week
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*1*12), # 12 adjusts for number of months per year
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.daily.perf.14, file = "fre.art.daily.perf.14.txt")



dat.temp<-subset(dat.14, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.seasonal.perf.14<-ddply(dat.temp, . (month, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*1), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*1), #1 adjusts for number of day-types per week
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*1), # 12 adjusts for number of months per year
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*1),
vkt.volume=sum(volume*(Length_m/1000))/(5*1),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*1),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*1)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.seasonal.perf.14, file = "fre.art.seasonal.perf.14.txt")





dat.temp<-subset(dat.14, subset = weekday.bin ==1 & hour == 17)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.seasonal.perf.14.5pm<-ddply(dat.temp, . (month, Freeway
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*1), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*1), #1 adjusts for number of day-types per week
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*1), # 12 adjusts for number of months per year
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*1),
vkt.volume=sum(volume*(Length_m/1000))/(5*1),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*1),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*1)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.seasonal.perf.14.5pm, file = "fre.art.seasonal.perf.14.5pm.txt")



dat.temp<-subset(dat.14, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
fre.art.seasonal.hourly.perf.14<-ddply(dat.temp, . (month, Freeway, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*1), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*1), #1 adjusts for number of day-types per week
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*1), # 12 adjusts for number of months per year
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*1),
vkt.volume=sum(volume*(Length_m/1000))/(5*1),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*1),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*1)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(fre.art.seasonal.hourly.perf.14, file = "fre.art.seasonal.hourly.perf.14.txt")













#DOWNTOWN TRENDS

#ALL THE 1'S ARE DOWNTOWN.  ALL ELSE (EVEN IF >0) IS NOT.


dat.temp<-subset(dat.11, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.perf.11.sep.nov<-ddply(dat.temp, . (hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.hourly.perf.11.sep.nov, file = "downtown.hourly.perf.11.sep.nov.txt")



dat.temp<-subset(dat.13, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.perf.13.sep.nov<-ddply(dat.temp, . (hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.hourly.perf.13.sep.nov, file = "downtown.hourly.perf.13.sep.nov.txt")



dat.temp<-subset(dat.14, subset = weekday.bin == 1& (month>8&month<12) & Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.perf.14.sep.nov<-ddply(dat.temp, . (hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.hourly.perf.14.sep.nov, file = "downtown.hourly.perf.14.sep.nov.txt")



dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.weekday.perf.14<-ddply(dat.temp, . (weekday
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*1*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.weekday.perf.14, file = "downtown.weekday.perf.14.txt")


dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1 & hour ==17)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.weekday.perf.14.5pm<-ddply(dat.temp, . (weekday
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*1*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.weekday.perf.14.5pm, file = "downtown.weekday.perf.14.5pm.txt")

dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.hourly.weekday.bin.perf.14<-ddply(dat.temp, . (weekday.bin, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*7*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.hourly.weekday.bin.perf.14, file = "downtown.hourly.weekday.bin.perf.14.txt")



dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1 & weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.corridor.hourly.perf.14<-ddply(dat.temp, . (CorridorUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.corridor.hourly.perf.14, file = "downtown.corridor.hourly.perf.14.txt")



dat.temp<-subset(dat.14, subset = Downtown_P == 1 & Freeway !=1 & weekday.bin == 1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
downtown.corridor.perf.14<-ddply(dat.temp, . (CorridorUID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(downtown.corridor.perf.14, file = "downtown.corridor.perf.14.txt")




#MULTILEVEL MODEL TO LOOK AT YEAR-OVER-YEAR DIFFERENCES.  

dat.temp<-subset(dat.14, subset = Downtown_P==1& Freeway !=1 & weekday.bin ==1)
lmer.14.downtown.seasonal<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month), 
               data = dat.temp, weights = weight.adj)

summary(lmer.14.downtown.seasonal)
fixef(lmer.14.downtown.seasonal)
ranef(lmer.14.downtown.seasonal)$month
ranef(lmer.14.downtown.seasonal)$weekday
ranef(lmer.14.downtown.seasonal)$tmc
ranef(lmer.14.downtown.seasonal)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.14.downtown.seasonal<-tidy(lmer.14.downtown.seasonal)
write.table(summary.lmer.14.downtown.seasonal, file = "summary.lmer.14.downtown.seasonal.txt")
write(fixef(lmer.14.downtown.seasonal), file = "fixef.lmer.14.downtown.seasonal.txt")
ranef.lmer.14.downtown.seasonal<-ranef(lmer.14.downtown.seasonal)$day.continuous
write.table(ranef.lmer.14.downtown.seasonal, file = "ranef.lmer.14.downtown.seasonal.txt")
rm(ranef.lmer.14.downtown.seasonal, summary.lmer.14.downtown.seasonal)
#rm(lmer.14.downtown.seasonal)


dat.temp<-subset(dat.14, subset = Downtown_P==1& Freeway !=1 & weekday.bin ==1)
lmer.14.downtown.seasonal.hourly<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|month:hour), 
                                data = dat.temp, weights = weight.adj)

summary(lmer.14.downtown.seasonal.hourly)
fixef(lmer.14.downtown.seasonal.hourly)
ranef(lmer.14.downtown.seasonal.hourly)$month
ranef(lmer.14.downtown.seasonal.hourly)$weekday
ranef(lmer.14.downtown.seasonal.hourly)$tmc
ranef(lmer.14.downtown.seasonal.hourly)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.14.downtown.seasonal.hourly<-tidy(lmer.14.downtown.seasonal.hourly)
write.table(summary.lmer.14.downtown.seasonal.hourly, file = "summary.lmer.14.downtown.seasonal.hourly.txt")
write(fixef(lmer.14.downtown.seasonal.hourly), file = "fixef.lmer.14.downtown.seasonal.hourly.txt")
ranef.lmer.14.downtown.seasonal.hourly<-ranef(lmer.14.downtown.seasonal.hourly)$day.continuous
write.table(ranef.lmer.14.downtown.seasonal.hourly, file = "ranef.lmer.14.downtown.seasonal.hourly.txt")
rm(ranef.lmer.14.downtown.seasonal.hourly, summary.lmer.14.downtown.seasonal.hourly)
#rm(lmer.14.downtown.seasonal.hourly)




dat.temp<-subset(dat.14, subset = Downtown_P==1& Freeway !=1 )
lmer.14.downtown.weekday<-lmer(speed.wtd~1+(1|tmc:month:hour)+ (1|weekday), 
                                       data = dat.temp, weights = weight.adj)

summary(lmer.14.downtown.weekday)
fixef(lmer.14.downtown.weekday)
ranef(lmer.14.downtown.weekday)$month
ranef(lmer.14.downtown.weekday)$weekday
ranef(lmer.14.downtown.weekday)$tmc
ranef(lmer.14.downtown.weekday)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.14.downtown.weekday<-tidy(lmer.14.downtown.weekday)
write.table(summary.lmer.14.downtown.weekday, file = "summary.lmer.14.downtown.weekday.txt")
write(fixef(lmer.14.downtown.weekday), file = "fixef.lmer.14.downtown.weekday.txt")
ranef.lmer.14.downtown.weekday<-ranef(lmer.14.downtown.weekday)$day.continuous
write.table(ranef.lmer.14.downtown.weekday, file = "ranef.lmer.14.downtown.weekday.txt")
rm(ranef.lmer.14.downtown.weekday, summary.lmer.14.downtown.weekday)
#rm(lmer.14.downtown.weekday)




dat.temp<-subset(dat.14, subset = Downtown_P==1& Freeway !=1  & hour ==17)
lmer.14.downtown.weekday.5pm<-lmer(speed.wtd~1+(1|tmc:month)+ (1|weekday), 
                               data = dat.temp, weights = weight.adj)

summary(lmer.14.downtown.weekday.5pm)
fixef(lmer.14.downtown.weekday.5pm)
ranef(lmer.14.downtown.weekday.5pm)$month
ranef(lmer.14.downtown.weekday.5pm)$weekday
ranef(lmer.14.downtown.weekday.5pm)$tmc
ranef(lmer.14.downtown.weekday.5pm)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.14.downtown.weekday.5pm<-tidy(lmer.14.downtown.weekday.5pm)
write.table(summary.lmer.14.downtown.weekday.5pm, file = "summary.lmer.14.downtown.weekday.5pm.txt")
write(fixef(lmer.14.downtown.weekday.5pm), file = "fixef.lmer.14.downtown.weekday.5pm.txt")
ranef.lmer.14.downtown.weekday.5pm<-ranef(lmer.14.downtown.weekday.5pm)$day.continuous
write.table(ranef.lmer.14.downtown.weekday.5pm, file = "ranef.lmer.14.downtown.weekday.5pm.txt")
rm(ranef.lmer.14.downtown.weekday.5pm, summary.lmer.14.downtown.weekday.5pm)
#rm(lmer.14.downtown.weekday.5pm)






lmer.downtown.all<-lmer(speed.wtd~1+(1|tmc:hour)+ (1|month)+(1|year:hour), 
               data = dat.all, weights = weight.adj.all, subset = Freeway !=1 & Downtown_P==1
               & weekday.bin ==1 & (month>8 & month<12))
summary(lmer.downtown.all)
fixef(lmer.downtown.all)
ranef(lmer.downtown.all)$month
ranef(lmer.downtown.all)$year
ranef(lmer.downtown.all)$tmc
ranef(lmer.downtown.all)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.downtown.all<-tidy(lmer.downtown.all)
write.table(summary.lmer.downtown.all, file = "summary.lmer.downtown.all.txt")
write(fixef(lmer.downtown.all), file = "fixef.lmer.downtown.all.txt")
ranef.lmer.downtown.all<-ranef(lmer.downtown.all)$day.continuous
write.table(ranef.lmer.downtown.all, file = "ranef.lmer.downtown.all.txt")
rm(ranef.lmer.downtown.all, summary.lmer.downtown.all)
#rm(lmer.downtown.all)




lmer.corridor.downtown.all<-lmer(speed.wtd~1+(1|tmc:hour)+ (1|month)+(1|CorridorUID:year:hour), 
                        data = dat.all, weights = weight.adj.all, subset = Freeway !=1 & Downtown_P==1
                        & CorridorUID>0 & weekday.bin ==1 & (month>8 & month<12))
summary(lmer.corridor.downtown.all)
fixef(lmer.corridor.downtown.all)
ranef(lmer.corridor.downtown.all)$month
ranef(lmer.corridor.downtown.all)$year
ranef(lmer.corridor.downtown.all)$tmc
ranef(lmer.corridor.downtown.all)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.corridor.downtown.all<-tidy(lmer.corridor.downtown.all)
write.table(summary.lmer.corridor.downtown.all, file = "summary.lmer.corridor.downtown.all.txt")
write(fixef(lmer.corridor.downtown.all), file = "fixef.lmer.corridor.downtown.all.txt")
ranef.lmer.corridor.downtown.all<-ranef(lmer.corridor.downtown.all)$day.continuous
write.table(ranef.lmer.corridor.downtown.all, file = "ranef.lmer.corridor.downtown.all.txt")
rm(ranef.lmer.corridor.downtown.all, summary.lmer.corridor.downtown.all)
#rm(lmer.downtown.all)


lmer.downtown.all.weekday.hourly<-lmer(speed.wtd~1+(1|tmc:hour:weekday.bin)+ (1|month)+(1|year:hour:weekday.bin), 
                                 data = dat.all, weights = weight.adj.all, subset = Freeway !=1 & Downtown_P==1
                                 & CorridorUID>0 & (month>8 & month<12))
summary(lmer.downtown.all.weekday.hourly)
fixef(lmer.downtown.all.weekday.hourly)
ranef(lmer.downtown.all.weekday.hourly)$month
ranef(lmer.downtown.all.weekday.hourly)$year
ranef(lmer.downtown.all.weekday.hourly)$tmc
ranef(lmer.downtown.all.weekday.hourly)
setwd("Y:/modeling/out/city_trends/report_card/")
summary.lmer.downtown.all.weekday.hourly<-tidy(lmer.downtown.all.weekday.hourly)
write.table(summary.lmer.downtown.all.weekday.hourly, file = "summary.lmer.downtown.all.weekday.hourly.txt")
write(fixef(lmer.downtown.all.weekday.hourly), file = "fixef.lmer.downtown.all.weekday.hourly.txt")
ranef.lmer.downtown.all.weekday.hourly<-ranef(lmer.downtown.all.weekday.hourly)$day.continuous
write.table(ranef.lmer.downtown.all.weekday.hourly, file = "ranef.lmer.downtown.all.weekday.hourly.txt")
rm(ranef.lmer.downtown.all.weekday.hourly, summary.lmer.downtown.all.weekday.hourly)




lmer.fre.year.hourly<-lmer(speed.wtd~1+(1|tmc:hour)+ (1|month)+(1|year:hour), 
                           data = dat.all, weights = weight.adj.all, subset = (Freeway ==1 & weekday.bin ==1))
summary(lmer.fre.year.hourly)
fixef(lmer.fre.year.hourly)
ranef(lmer.fre.year.hourly)$month
ranef(lmer.fre.year.hourly)$weekday
ranef(lmer.fre.year.hourly)$tmc
ranef(lmer.fre.year.hourly)
setwd("Y:/modeling/out/city_trends/")
summary.lmer.fre.year.hourly<-tidy(lmer.fre.year.hourly)
write.table(summary.lmer.fre.year.hourly, file = "summary.lmer.fre.year.hourly.txt")
write(fixef(lmer.fre.year.hourly), file = "fixef.lmer.fre.year.hourly.txt")
ranef.lmer.fre.year.hourly<-ranef(lmer.fre.year.hourly)$day.continuous
write.table(ranef.lmer.fre.year.hourly, file = "ranef.lmer.fre.year.hourly.txt")
rm(ranef.lmer.fre.year.hourly, summary.lmer.fre.year.hourly)
#rm(lmer.fre.year.hourly)








lmer.downtown.all1<-lmer(speed.wtd~1+(1|tmc:hour:year)+ (1|month), 
                         data = dat.all, weights = weight.adj.all, subset = Freeway !=1 & Downtown_P==1
                         & weekday.bin ==1 & (month>8 & month<12))
summary(lmer.downtown.all1)
fixef(lmer.downtown.all1)
ranef(lmer.downtown.all1)$month
ranef(lmer.downtown.all1)$year
ranef(lmer.downtown.all1)$tmc
ranef(lmer.downtown.all1)
setwd("Y:/modeling/out/city_trends/report_card/")

summary.lmer.downtown.all1<-tidy(lmer.downtown.all1)
write.table(summary.lmer.downtown.all1, file = "summary.lmer.downtown.all1.txt")
write(fixef(lmer.downtown.all1), file = "fixef.lmer.downtown.all1.txt")
ranef.lmer.downtown.all1<-ranef(lmer.downtown.all1)$day.continuous
write.table(ranef.lmer.downtown.all1, file = "ranef.lmer.downtown.all1.txt")
rm(ranef.lmer.downtown.all1, summary.lmer.downtown.all1)
#rm(lmer.downtown.all1)


#EXTRACTING THE RESULTS FOR FURTHER PROCESSING:
summary.lmer.downtown.all1<-tidy(lmer.downtown.all1)
n.3<-(dim(summary.lmer.downtown.all1)[1]-3)
n<-dim(summary.lmer.downtown.all1)[1]

summary.lmer.downtown.all1.month<-summary.lmer.downtown.all1[(n.3+1):n,]
summary.lmer.downtown.all1<-summary.lmer.downtown.all1[1:n.3,]
dim(summary.lmer.downtown.all1)
names(summary.lmer.downtown.all1)
level1<-colsplit(summary.lmer.downtown.all1$level,":", c("tmc", "hour", "year"))
summary.lmer.downtown.all1<-data.frame(cbind(level1, summary.lmer.downtown.all1$value))
names(summary.lmer.downtown.all1)<-c("tmc", "hour", "year", "speed")
summary.lmer.downtown.all1.month<-data.frame(summary.lmer.downtown.all1.month[,c("level", "value")])
names(summary.lmer.downtown.all1.month)<-c("month", "speed")
summary.lmer.downtown.all1.month
summary.lmer.downtown.all1.month$speed<-summary.lmer.downtown.all1.month$speed*1.60934
summary.lmer.downtown.all1$speed<-summary.lmer.downtown.all1$speed*1.60934


summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, dat.tmc.newADD, by = "tmc", all = FALSE)
summary.lmer.downtown.all1$newADD<-as.numeric(summary.lmer.downtown.all1$newADD)
hist(summary.lmer.downtown.all1$newADD)
summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, dat.speed85, by = "tmc" , all = FALSE)
summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, torNetwork_UIDs_feb10, by = "tmc" , all = FALSE)
dat.final.net1<-dat.final.net[,c("newADD", "Freeway", "Length_m")]
summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, dat.final.net1, by = "newADD", all = FALSE)
rm(dat.final.net1)
year<-rep(2014, length = dim(dat.14.long)[1])
dat.14.long1<-data.frame(cbind(dat.14.long, year))
year<-rep(2013, length = dim(dat.13.long)[1])
dat.13.long1<-data.frame(cbind(dat.13.long, year))
year<-rep(2011, length = dim(dat.11.long)[1])
dat.11.long1<-data.frame(cbind(dat.11.long, year))
rm(year)
dat.long<-data.frame(rbind(dat.11.long1, dat.13.long1, dat.14.long1))
rm(dat.13.long1, dat.14.long1, dat.11.long1)

summary.lmer.downtown.all1<-merge(summary.lmer.downtown.all1, dat.long, by = c("hour", "newADD", "year"), all = FALSE)

dim(summary.lmer.downtown.all1)

#all is adjusted to kph
summary.lmer.downtown.all1$speed85<-summary.lmer.downtown.all1$speed85*1.60934
summary.lmer.downtown.all1$night.speed<-summary.lmer.downtown.all1$night.speed*1.60934


#COUNT ADJUSTMENTS ARE CALCULATED HERE.  
weekday.bin<-rep(1,length =dim(summary.lmer.downtown.all1)[1])
month<-rep(0, length = dim(summary.lmer.downtown.all1)[1])
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, month, weekday.bin))
rm(month, weekday.bin)
summary.lmer.downtown.all1$month<-9
count.adj.9<-4*predict(lm.11a, summary.lmer.downtown.all1)
summary.lmer.downtown.all1$month<-10
count.adj.10<-4*predict(lm.11a, summary.lmer.downtown.all1)
summary.lmer.downtown.all1$month<-11
count.adj.11<-4*predict(lm.11a, summary.lmer.downtown.all1)
count.adj<-(count.adj.9+ count.adj.10+ count.adj.11)/3
hist(count.adj)
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, count.adj))
rm(count.adj, count.adj.9, count.adj.10, count.adj.11)

#PROCESSING SO THAT DELAY IS NEVER NEGATIVE
speed1<-summary.lmer.downtown.all1$speed
speed1<-pmin(speed1, summary.lmer.downtown.all1$speed85)
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, speed1))
rm(speed1)

speed2<-summary.lmer.downtown.all1$speed
speed2<-pmin(speed2, summary.lmer.downtown.all1$night.speed)
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, speed2))
rm(speed2)

vkt.count.adj<-summary.lmer.downtown.all1$count.adj*(summary.lmer.downtown.all1$Length_m/1000)
vkt.count.adj.speed<-vkt.count.adj/summary.lmer.downtown.all1$speed
vkt.count.adj.speed1<-vkt.count.adj/summary.lmer.downtown.all1$speed1
vkt.count.adj.speed2<-vkt.count.adj/summary.lmer.downtown.all1$speed2
vkt.count.adj.speed85<-vkt.count.adj/summary.lmer.downtown.all1$speed85
vkt.count.adj.night.speed<-vkt.count.adj/summary.lmer.downtown.all1$night.speed

summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, vkt.count.adj, vkt.count.adj.speed, vkt.count.adj.speed1, 
                                   vkt.count.adj.speed2, vkt.count.adj.speed85,
                                   vkt.count.adj.night.speed))
rm(vkt.count.adj, vkt.count.adj.speed, vkt.count.adj.speed1, 
   vkt.count.adj.speed2, vkt.count.adj.speed85,
   vkt.count.adj.night.speed)
rm(n, n.3)
#INTEGRATING MTO ADJUSTMENTS FOR 2011.  NEEDS TO BE DONE FOR OTHER YEARS AS WELL#####


CorridorUID1<-summary.lmer.downtown.all1$CorridorUID
CorridorUID1[CorridorUID1!=81 &CorridorUID1!=82&CorridorUID1!=80&CorridorUID1!=88
             &CorridorUID1!=85&CorridorUID1!=83]<-88
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, CorridorUID1))
rm(CorridorUID1)
weekday<-rep(5, length  = dim(summary.lmer.downtown.all1)[1])  #SO THE SIMULATION IS FOR THURSDAY (WEEKDAY =5)
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, weekday))
rm(weekday)
count.adj1<-predict(lm.b, summary.lmer.downtown.all1)
count.adj1[count.adj1<1]<-0
count.adj2<-predict(lm.c, summary.lmer.downtown.all1)

bin.1<-rep(0, length = length(count.adj1))
bin.1[summary.lmer.downtown.all1$CorridorUID==80 | summary.lmer.downtown.all1$CorridorUID==81 | summary.lmer.downtown.all1$CorridorUID==82 | 
        summary.lmer.downtown.all1$CorridorUID==83 | summary.lmer.downtown.all1$CorridorUID==85 | summary.lmer.downtown.all1$CorridorUID==88]<-1
bin.2<-rep(0, length = length(count.adj1))
bin.2[summary.lmer.downtown.all1$CorridorUID==87 | summary.lmer.downtown.all1$CorridorUID==89 ]<-1
bin.3<-rep(0, length= length(count.adj1))
bin.3[bin.1==0&bin.2==0]<-1
count.adj.all<-summary.lmer.downtown.all1$count.adj*bin.3+count.adj1*bin.1+count.adj2*bin.2
summary.lmer.downtown.all1<-data.frame(cbind(summary.lmer.downtown.all1, count.adj.all))
names(summary.lmer.downtown.all1)
rm(bin.1, bin.2, bin.3, count.adj.all, count.adj1, count.adj2)

setwd("Y:/modeling/out/city_trends/report_card/") #sets the working directory.
write.table(summary.lmer.downtown.all1, file = "summary.lmer.downtown.all1.proc.txt")



#CORRIDOR ANALYSES


dat.temp<-subset(dat.14, subset = CorridorUID>0& weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14<-ddply(dat.temp, . (CorridorUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.14, file = "corridor.hourly.perf.14.txt")


dat.temp<-subset(dat.14, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.sep.nov<-ddply(dat.temp, . (CorridorUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.14.sep.nov, file = "corridor.hourly.perf.14.sep.nov.txt")




dat.temp<-subset(dat.13, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.13.sep.nov<-ddply(dat.temp, . (CorridorUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.13.sep.nov, file = "corridor.hourly.perf.13.sep.nov.txt")




dat.temp<-subset(dat.11, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.11.sep.nov<-ddply(dat.temp, . (CorridorUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.11.sep.nov, file = "corridor.hourly.perf.11.sep.nov.txt")






dat.temp<-subset(dat.14, subset = CorridorUID>0& weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.seasonal.perf.14<-ddply(dat.temp, . (C_UID, month
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*1), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*1),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*1),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*1),
vkt.volume=sum(volume*(Length_m/1000))/(5*1),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*1),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*1)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.seasonal.perf.14, file = "corridor.directional.seasonal.perf.14.txt")



dat.temp<-subset(dat.14, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.14.sep.nov<-ddply(dat.temp, . (C_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.14.sep.nov, file = "corridor.directional.hourly.perf.14.sep.nov.txt")




dat.temp<-subset(dat.13, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.13.sep.nov<-ddply(dat.temp, . (C_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.13.sep.nov, file = "corridor.directional.hourly.perf.13.sep.nov.txt")




dat.temp<-subset(dat.11, subset = CorridorUID>0 & weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.11.sep.nov<-ddply(dat.temp, . (C_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.11.sep.nov, file = "corridor.directional.hourly.perf.11.sep.nov.txt")


dat.temp<-subset(dat.14, subset = weekday.bin ==1 )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.hourly.perf.14<-ddply(dat.temp, . (hour, C_UID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)

corridor.directional.hourly.perf.14

setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.14, file = "corridor.directionally.hourly.perf.14.txt")









#BELOW ARE SOME AD HOC ANALYSES WHICH NEED TO BE RUN TO EXTRACT QUEEN AND COLLEGE CORRECTLY, AND ALSO TO EXTRACT THE TWO-PLUS STREET CORRIDORS (E.G. ALBION/WILSON, ETC.)
#2014 adjustments
dat.14.0812<-subset(dat.14, subset = (
  P_UID == 19.1 | P_UID == 19.2 | P_UID == 13.1 | P_UID == 13.2 | C_UID == 2.1 | C_UID == 2.2 | C_UID == 74.1 | C_UID == 74.2 |
    C_UID == 20.3 | C_UID == 20.4 | C_UID == 12.3 | C_UID == 12.4 | C_UID == 29.1 | C_UID == 29.2 | C_UID == 84.1 | C_UID == 84.2 | 
    C_UID == 33.3| C_UID == 33.4 | C_UID == 63.3 | C_UID == 63.4 | P_UID == 17.1 | P_UID == 17.2 | C_UID == 78.1 | C_UID == 78.2 | 
    C_UID == 28.1 | C_UID == 28.2))
#P_UID and C_UID are numeric.  
dim(dat.14.0812)
dat.14.0812$Ponly_bin<-0
dat.14.0812$Ponly_bin[dat.14.0812$P_UID == 19.1 | dat.14.0812$P_UID == 19.2 | dat.14.0812$P_UID == 13.1 | dat.14.0812$P_UID == 13.2 | dat.14.0812$P_UID == 17.1 | dat.14.0812$P_UID == 17.2]<-1
dat.14.0812$noP_bin<-0
dat.14.0812$noP_bin[dat.14.0812$Ponly_bin== 0]<-1

dat.14.0812$LengthUID<-dat.14.0812$noP_bin*dat.14.0812$CorridorUID + dat.14.0812$Ponly_bin*floor(dat.14.0812$P_UID)
dat.14.0812$L_UID<-dat.14.0812$noP_bin*dat.14.0812$C_UID + dat.14.0812$Ponly_bin*dat.14.0812$P_UID
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 2 | dat.14.0812$LengthUID == 74]<-2
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 20 | dat.14.0812$LengthUID == 12]<-12
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 29 | dat.14.0812$LengthUID == 84]<-29
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 33 | dat.14.0812$LengthUID == 63]<-33
dat.14.0812$LengthUID[dat.14.0812$LengthUID == 78 | dat.14.0812$LengthUID == 28]<-28


dat.14.0812$L_UID[dat.14.0812$L_UID == 2.1 | dat.14.0812$L_UID == 74.1]<-2.1
dat.14.0812$L_UID[dat.14.0812$L_UID == 20.3 | dat.14.0812$L_UID == 12.3]<-12.3
dat.14.0812$L_UID[dat.14.0812$L_UID == 29.1 | dat.14.0812$L_UID == 84.1]<-29.1
dat.14.0812$L_UID[dat.14.0812$L_UID == 33.3 | dat.14.0812$L_UID == 63.3]<-33.3
dat.14.0812$L_UID[dat.14.0812$L_UID == 78.1 | dat.14.0812$L_UID == 28.1]<-28.1

dat.14.0812$L_UID[dat.14.0812$L_UID == 2.2 | dat.14.0812$L_UID == 74.2]<-2.2
dat.14.0812$L_UID[dat.14.0812$L_UID == 20.4 | dat.14.0812$L_UID == 12.4]<-12.4
dat.14.0812$L_UID[dat.14.0812$L_UID == 29.2 | dat.14.0812$L_UID == 84.2]<-29.2
dat.14.0812$L_UID[dat.14.0812$L_UID == 33.4 | dat.14.0812$L_UID == 63.4]<-33.4
dat.14.0812$L_UID[dat.14.0812$L_UID == 78.2 | dat.14.0812$L_UID == 28.2]<-28.2



#BELOW ARE SOME AD HOC ANALYSES WHICH NEED TO BE RUN TO EXTRACT QUEEN AND COLLEGE CORRECTLY, AND ALSO TO EXTRACT THE TWO-PLUS STREET CORRIDORS (E.G. ALBION/WILSON, ETC.)
#2013 adjustments
dat.13.0812<-subset(dat.13, subset = (
  P_UID == 19.1 | P_UID == 19.2 | P_UID == 13.1 | P_UID == 13.2 | C_UID == 2.1 | C_UID == 2.2 | C_UID == 74.1 | C_UID == 74.2 |
    C_UID == 20.3 | C_UID == 20.4 | C_UID == 12.3 | C_UID == 12.4 | C_UID == 29.1 | C_UID == 29.2 | C_UID == 84.1 | C_UID == 84.2 | 
    C_UID == 33.3| C_UID == 33.4 | C_UID == 63.3 | C_UID == 63.4 | P_UID == 17.1 | P_UID == 17.2 | C_UID == 78.1 | C_UID == 78.2 | 
    C_UID == 28.1 | C_UID == 28.2))
#P_UID and C_UID are numeric.  
dim(dat.13.0812)
dat.13.0812$Ponly_bin<-0
dat.13.0812$Ponly_bin[dat.13.0812$P_UID == 19.1 | dat.13.0812$P_UID == 19.2 | dat.13.0812$P_UID == 13.1 | dat.13.0812$P_UID == 13.2 | dat.13.0812$P_UID == 17.1 | dat.13.0812$P_UID == 17.2]<-1
dat.13.0812$noP_bin<-0
dat.13.0812$noP_bin[dat.13.0812$Ponly_bin== 0]<-1

dat.13.0812$LengthUID<-dat.13.0812$noP_bin*dat.13.0812$CorridorUID + dat.13.0812$Ponly_bin*floor(dat.13.0812$P_UID)
dat.13.0812$L_UID<-dat.13.0812$noP_bin*dat.13.0812$C_UID + dat.13.0812$Ponly_bin*dat.13.0812$P_UID
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 2 | dat.13.0812$LengthUID == 74]<-2
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 20 | dat.13.0812$LengthUID == 12]<-12
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 29 | dat.13.0812$LengthUID == 84]<-29
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 33 | dat.13.0812$LengthUID == 63]<-33
dat.13.0812$LengthUID[dat.13.0812$LengthUID == 78 | dat.13.0812$LengthUID == 28]<-28


dat.13.0812$L_UID[dat.13.0812$L_UID == 2.1 | dat.13.0812$L_UID == 74.1]<-2.1
dat.13.0812$L_UID[dat.13.0812$L_UID == 20.3 | dat.13.0812$L_UID == 12.3]<-12.3
dat.13.0812$L_UID[dat.13.0812$L_UID == 29.1 | dat.13.0812$L_UID == 84.1]<-29.1
dat.13.0812$L_UID[dat.13.0812$L_UID == 33.3 | dat.13.0812$L_UID == 63.3]<-33.3
dat.13.0812$L_UID[dat.13.0812$L_UID == 78.1 | dat.13.0812$L_UID == 28.1]<-28.1

dat.13.0812$L_UID[dat.13.0812$L_UID == 2.2 | dat.13.0812$L_UID == 74.2]<-2.2
dat.13.0812$L_UID[dat.13.0812$L_UID == 20.4 | dat.13.0812$L_UID == 12.4]<-12.4
dat.13.0812$L_UID[dat.13.0812$L_UID == 29.2 | dat.13.0812$L_UID == 84.2]<-29.2
dat.13.0812$L_UID[dat.13.0812$L_UID == 33.4 | dat.13.0812$L_UID == 63.4]<-33.4
dat.13.0812$L_UID[dat.13.0812$L_UID == 78.2 | dat.13.0812$L_UID == 28.2]<-28.2



#BELOW ARE SOME AD HOC ANALYSES WHICH NEED TO BE RUN TO EXTRACT QUEEN AND COLLEGE CORRECTLY, AND ALSO TO EXTRACT THE TWO-PLUS STREET CORRIDORS (E.G. ALBION/WILSON, ETC.)
#2011 adjustments
dat.11.0812<-subset(dat.11, subset = (
  P_UID == 19.1 | P_UID == 19.2 | P_UID == 13.1 | P_UID == 13.2 | C_UID == 2.1 | C_UID == 2.2 | C_UID == 74.1 | C_UID == 74.2 |
    C_UID == 20.3 | C_UID == 20.4 | C_UID == 12.3 | C_UID == 12.4 | C_UID == 29.1 | C_UID == 29.2 | C_UID == 84.1 | C_UID == 84.2 | 
    C_UID == 33.3| C_UID == 33.4 | C_UID == 63.3 | C_UID == 63.4 | P_UID == 17.1 | P_UID == 17.2 | C_UID == 78.1 | C_UID == 78.2 | 
    C_UID == 28.1 | C_UID == 28.2))
#P_UID and C_UID are numeric.  
dim(dat.11.0812)
dat.11.0812$Ponly_bin<-0
dat.11.0812$Ponly_bin[dat.11.0812$P_UID == 19.1 | dat.11.0812$P_UID == 19.2 | dat.11.0812$P_UID == 13.1 | dat.11.0812$P_UID == 13.2 | dat.11.0812$P_UID == 17.1 | dat.11.0812$P_UID == 17.2]<-1
dat.11.0812$noP_bin<-0
dat.11.0812$noP_bin[dat.11.0812$Ponly_bin== 0]<-1

dat.11.0812$LengthUID<-dat.11.0812$noP_bin*dat.11.0812$CorridorUID + dat.11.0812$Ponly_bin*floor(dat.11.0812$P_UID)
dat.11.0812$L_UID<-dat.11.0812$noP_bin*dat.11.0812$C_UID + dat.11.0812$Ponly_bin*dat.11.0812$P_UID
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 2 | dat.11.0812$LengthUID == 74]<-2
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 20 | dat.11.0812$LengthUID == 12]<-12
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 29 | dat.11.0812$LengthUID == 84]<-29
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 33 | dat.11.0812$LengthUID == 63]<-33
dat.11.0812$LengthUID[dat.11.0812$LengthUID == 78 | dat.11.0812$LengthUID == 28]<-28


dat.11.0812$L_UID[dat.11.0812$L_UID == 2.1 | dat.11.0812$L_UID == 74.1]<-2.1
dat.11.0812$L_UID[dat.11.0812$L_UID == 20.3 | dat.11.0812$L_UID == 12.3]<-12.3
dat.11.0812$L_UID[dat.11.0812$L_UID == 29.1 | dat.11.0812$L_UID == 84.1]<-29.1
dat.11.0812$L_UID[dat.11.0812$L_UID == 33.3 | dat.11.0812$L_UID == 63.3]<-33.3
dat.11.0812$L_UID[dat.11.0812$L_UID == 78.1 | dat.11.0812$L_UID == 28.1]<-28.1

dat.11.0812$L_UID[dat.11.0812$L_UID == 2.2 | dat.11.0812$L_UID == 74.2]<-2.2
dat.11.0812$L_UID[dat.11.0812$L_UID == 20.4 | dat.11.0812$L_UID == 12.4]<-12.4
dat.11.0812$L_UID[dat.11.0812$L_UID == 29.2 | dat.11.0812$L_UID == 84.2]<-29.2
dat.11.0812$L_UID[dat.11.0812$L_UID == 33.4 | dat.11.0812$L_UID == 63.4]<-33.4
dat.11.0812$L_UID[dat.11.0812$L_UID == 78.2 | dat.11.0812$L_UID == 28.2]<-28.2



dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.0812<-ddply(dat.temp, . (LengthUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.14.0812, file = "corridor.hourly.perf.14.0812.txt")


dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.14.sep.nov.0812<-ddply(dat.temp, . (LengthUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.14.sep.nov.0812, file = "corridor.hourly.perf.14.sep.nov.0812.txt")












dat.temp<-subset(dat.13.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.13.sep.nov.0812<-ddply(dat.temp, . (LengthUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.13.sep.nov.0812, file = "corridor.hourly.perf.13.sep.nov.0812.txt")




dat.temp<-subset(dat.11.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.hourly.perf.11.sep.nov.0812<-ddply(dat.temp, . (LengthUID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.hourly.perf.11.sep.nov.0812, file = "corridor.hourly.perf.11.sep.nov.0812.txt")






dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1)
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.seasonal.perf.14.0812<-ddply(dat.temp, . (L_UID, month
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*1), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*1),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*1),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*1),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*1),
vkt.volume=sum(volume*(Length_m/1000))/(5*1),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*1),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*1)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.seasonal.perf.14.0812, file = "corridor.directional.seasonal.perf.14.0812.txt")



dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.14.sep.nov.0812<-ddply(dat.temp, . (L_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.14.sep.nov.0812, file = "corridor.directional.hourly.perf.14.sep.nov.0812.txt")




dat.temp<-subset(dat.13.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.13.sep.nov.0812<-ddply(dat.temp, . (L_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.13.sep.nov.0812, file = "corridor.directional.hourly.perf.13.sep.nov.0812.txt")




dat.temp<-subset(dat.11.0812, subset = weekday.bin ==1 & (month>8&month<12))
dat.temp$Freeway[dat.temp$Freeway==2]<-0
corridor.directional.hourly.perf.11.sep.nov.0812<-ddply(dat.temp, . (L_UID, hour
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*3), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*3),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*3),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*3),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*3),
vkt.volume=sum(volume*(Length_m/1000))/(5*3),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*3),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*3)
)
setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.11.sep.nov.0812, file = "corridor.directional.hourly.perf.11.sep.nov.0812.txt")


dat.temp<-subset(dat.14.0812, subset = weekday.bin ==1 )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.hourly.perf.14.0812<-ddply(dat.temp, . (hour, L_UID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*5*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*5*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*5*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*5*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*5*12),
vkt.volume=sum(volume*(Length_m/1000))/(5*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(5*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(5*12)
)

corridor.directional.hourly.perf.14.0812

setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.hourly.perf.14.0812, file = "corridor.directional.hourly.perf.14.0812.txt")




dat.temp<-subset(dat.14.0812, subset = weekday.bin !="NA" )
dat.temp$CorridorUID[dat.temp$CorridorUID==29]<-84
corridor.directional.weekday.perf.14.0812<-ddply(dat.temp, . (hour, weekday, L_UID
),
summarise,
tti.85.volume= (sum(volume*Length_m/speed.wtd1)/sum(volume*Length_m/speed85)),
tti.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)/sum(count.adj*Length_m/speed85)),
tti.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)/sum(count.adj.all*Length_m/speed85)),
tti.night.volume= (sum(volume*Length_m/speed.wtd2)/sum(volume*Length_m/night.speed)),
tti.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)/sum(count.adj*Length_m/night.speed)),
tti.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)/sum(count.adj.all*Length_m/night.speed)) , 
speed.volume=(sum(volume*Length_m*speed.wtd1)/sum(volume*Length_m)),
speed.count.adj=(sum(count.adj*Length_m*speed.wtd1)/sum(count.adj*Length_m)),
speed.count.adj.all=(sum(count.adj.all*Length_m*speed.wtd1)/sum(count.adj.all*Length_m)),
delay.85.volume= (sum(volume*Length_m/speed.wtd1)-sum(volume*Length_m/speed85))/(1609.34*1*12), #1609.34 adjusts from meters to miles to normalize
delay.85.count.adj= (sum(count.adj*Length_m/speed.wtd1)-sum(count.adj*Length_m/speed85))/(1609.34*1*12),
delay.85.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd1)-sum(count.adj.all*Length_m/speed85))/(1609.34*1*12),
delay.night.volume= (sum(volume*Length_m/speed.wtd2)-sum(volume*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj= (sum(count.adj*Length_m/speed.wtd2)-sum(count.adj*Length_m/night.speed))/(1609.34*1*12),
delay.night.count.adj.all= (sum(count.adj.all*Length_m/speed.wtd2)-sum(count.adj.all*Length_m/night.speed))/(1609.34*1*12),
vkt.volume=sum(volume*(Length_m/1000))/(1*12),
vkt.count.adj=sum(count.adj*(Length_m/1000))/(1*12),
vkt.count.adj.all=sum(count.adj.all*(Length_m/1000))/(1*12)
)

corridor.directional.weekday.perf.14.0812

setwd("Y:/modeling/out/corridors/") #sets the working directory.
write.table(corridor.directional.weekday.perf.14.0812, file = "corridor.directional.weekday.perf.14.0812.txt")






#FOLLOWING IS CODE TO DOUBLE-CHECK WHAT'S GOING ON WITH THE GARDINER.


dat.gardiner.wb<-subset(dat.all, subset = C_UID == 29.2 | C_UID == 84.2)
dim(dat.gardiner.wb)
hist(dat.gardiner.wb$year)
dat.temp<-subset(dat.gardiner.wb, subset = (month<12 & month >8))
dim(dat.temp)
hist(dat.temp$year)


lmer.gardiner.wb<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|hour:month:year) + (1|month) + (1|year) + (1|hour:weekday.bin), 
               data = dat.gardiner, weights = weight.adj.all, subset = (month<12 & month >8))
ranef(lmer.gardiner.wb)

dat.gardiner.wb$tti.85<-dat.gardiner.wb$speed85/dat.gardiner.wb$speed.wtd1
lmer.gardiner.wb.tti<-lmer(tti.85~1+(1|tmc:hour:weekday)+ (1|hour:month:year) + (1|month) + (1|year) + (1|hour:weekday.bin), 
                       data = dat.gardiner.wb, weights = weight.adj.all, subset = (month<12 & month >8))
ranef(lmer.gardiner.wb.tti)
fixef(lmer.gardiner.wb.tti)



dat.gardiner.eb<-subset(dat.all, subset = C_UID == 29.1 | C_UID == 84.1)
dim(dat.gardiner.eb)
hist(dat.gardiner.eb$year)
dat.temp<-subset(dat.gardiner.eb, subset = (month<12 & month >8))
dim(dat.temp)
hist(dat.temp$year)


lmer.gardiner.eb<-lmer(speed.wtd~1+(1|tmc:hour:weekday)+ (1|hour:month:year) + (1|month) + (1|year) + (1|hour:weekday.bin), 
                    data = dat.gardiner.eb, weights = weight.adj.all, subset = ((month<12 & month >8) & (hour == 8 | hour == 17)))
ranef(lmer.gardiner.eb)

dat.gardiner.eb$tti.85<-dat.gardiner.eb$speed85/dat.gardiner.eb$speed.wtd1
lmer.gardiner.eb.tti<-lmer(tti.85~1+(1|tmc:hour:weekday)+ (1|hour:month:year) + (1|month) + (1|year) + (1|hour:weekday.bin), 
                           data = dat.gardiner.eb, weights = weight.adj.all, subset = (month<12 & month >8))
ranef(lmer.gardiner.eb.tti)
fixef(lmer.gardiner.eb.tti)




lmer.gardiner.eb.tti1<-lmer(tti.85~1+(1|tmc:weekday)+ (1|month:year) , 
                           data = dat.gardiner.eb, weights = weight.adj.all, subset = (month<12 & month >8 & hour == 8))
ranef(lmer.gardiner.eb.tti1)
fixef(lmer.gardiner.eb.tti1)


lmer.gardiner.eb.1<-lmer(speed.wtd~1+(1|tmc:weekday)+ (1|month:year) , 
                            data = dat.gardiner.eb, weights = weight.adj.all, subset = (month<12 & month >8 & hour == 8))
ranef(lmer.gardiner.eb.1)
fixef(lmer.gardiner.eb.1)


