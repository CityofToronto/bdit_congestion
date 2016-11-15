library(RPostgreSQL)
library(plyr)
library(broom)
library(ggplot2)
library(stringr)
library(lme4)

analysis_year <- 2014
freeway_flag <- 0
dir.create(file.path(paste0(getwd(),"/out"), paste0(analysis_year,"_daily")), showWarnings = FALSE)
all_months <- c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
seasons <- c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
             "autumn", "autumn", "autumn")

################################
# IMPORT FROM POSTGRESQL
################################
drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

# DETERMINE MONTHS OF DATA
# strSQL = paste("SELECT DISTINCT EXTRACT(MONTH FROM EHR.dt) as month ",
#                "FROM inrix.agg_extract_hour EHR ",
#                "WHERE EXTRACT(YEAR FROM EHR.dt) = ",
#                analysis_year,
#                sep = '')
# 
# avail_months <- dbGetQuery(con, strSQL)
# avail_months <- avail_months$month[order(avail_months$month)]
avail_months = 1:12

for (i in avail_months) {
  strSQL = paste("SELECT TMC.tmc, TMC.length_m, ", 
                 "DATE_TRUNC('day', EHR.datetime_bin) AS day_only, ",
                 "EHR.weekday, VOL.hour, ",
                 "SUM(EHR.speed * VOL.volume * TMC.length_m) / SUM(VOL.volume * TMC.length_m) AS speedwtd, ",
                 "AVG(VOL.volume) AS vol ",
                 "FROM inrix.volumes VOL ",
                 "INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id ",
                 "INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc ",
                 "WHERE EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour ",
                 "AND TMC.freeway = ",freeway_flag," ",
                 "AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
                 " AND EHR.month = ",i,
                 " GROUP BY TMC.tmc, TMC.length_m, DATE_TRUNC('day', EHR.datetime_bin), ",
                 "EHR.month, EHR.weekday, VOL.hour",
                 sep = '')
  dat.temp <- dbGetQuery(con, strSQL)
  write.csv(dat.temp, file = paste0("in/",analysis_year,"/hourly_",analysis_year,"_",sprintf("%02d",i),".csv"))
}
  
################################
# MONTHLY MLM ESTIMATION
################################
for (i in avail_months) {
  
  dat.hour <- read.csv(file = paste0("in/",analysis_year,"/hourly_",analysis_year,"_",sprintf("%02d",i),".csv"))
  dat.hour$weight <- (dat.hour$vol*dat.hour$length_m) / mean((dat.hour$vol*dat.hour$length_m))
    
  lmer.hour <- lmer( speedwtd ~ 1 +
                       (1|tmc:hour)
                     + (1|day_only)
                     , data = dat.hour
                     , weights = weight)

  filename = paste("out/models/summary.lmer.hour.",i,".txt",sep='')
  write.table(tidy(lmer.hour), file = filename)
  
  filename = paste("out/models/fixef.lmer.hour.",i,".txt",sep='')
  write(fixef(lmer.hour), file = filename)
  
  filename = paste("out/models/ranef.lmer.hour.",i,".txt",sep='')
  write.table(ranef(lmer.hour)$day_only, file = filename)
  
  rm(dat.hour, lmer.hour)
}

########################################
# READ AND STORE MODEL RESULTS
########################################

#########
# FIXED #
#########
fixef <- NULL
for (i in avail_months) {
  temp <- read.table(paste("out/models/fixef.lmer.hour.", i,  ".txt", sep=""), col.names = c("speed.mean.monthly"))
  fixef <- data.frame(rbind(fixef,temp))
}
fixef$month <- avail_months

######################
# SPEED DIFF EFFECTS #
######################
ranef <- NULL
for (i in avail_months) {
  temp <- read.table(paste("out/models/ranef.lmer.hour.", i,  ".txt", sep=""), col.names = c("date","speed.dif"), skip = 1)
  ranef <- data.frame(rbind(ranef,temp))
}
ranef$date <- as.Date(ranef$date)

#####################
# DAILY MEAN SPEEDS #
#####################
ranef$month <- as.numeric(format(ranef$date, "%m"))
dat.daily <- merge(ranef, fixef, by = "month")
dat.daily$mean.daily.speed <- dat.daily$speed.mean.monthly + dat.daily$speed.dif

dat.date.lookup <- read.table("in/date_lookup_full.csv",header=TRUE, sep = ",")
dat.date.lookup <- subset(dat.date.lookup, subset = (year == analysis_year))
dat.date.lookup$date <- as.Date(as.character(dat.date.lookup$date), format = "%d-%b-%y")

dat.daily <- merge(dat.daily, dat.date.lookup, by =  "date")
rm(dat.date.lookup)

###############################
# ADDITIONAL FIELDS
###############################
dat.daily <- rename(dat.daily, c("month.x"="month"))
months <- NULL
months$month <- avail_months
months$month.string <- all_months[avail_months]
dat.daily <- merge(dat.daily, months, by = "month")
rm(months)
dat.daily$month.y <- NULL

weekday <- 1:7
weekday.string <- c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday <- 1:7
weekday.type <- c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types <- data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

dat.daily$mean.daily.speed.kph <- dat.daily$mean.daily.speed*1.60934
seasonality <- NULL
seasonality$month <- avail_months
seasonality$season <- seasons[avail_months]
dat.daily <- merge(dat.daily, seasonality, by = "month")
rm(seasonality)

write.table(dat.daily, file = paste("out/",analysis_year,"_daily/dat.daily.txt", sep =""))


######################################
# MONTHLY MODEL ESTIMATION -  PLOTS
#####################################

# MONTHLY VARIATIONS PLOT
ggplot.monthly <- ggplot(data = dat.daily, aes(x=day, y=mean.daily.speed.kph, colour = factor(weekday.type))) +
  ggtitle(paste('Daily Variations by Month (',analysis_year,')',sep="")) +
  theme(plot.title=element_text(size = 16, face = "bold", vjust=2)) +
  geom_point() +
  facet_wrap(~month) +
  theme(legend.title = element_text(colour="black", size=12, face="bold")) +
  scale_color_discrete(name="Weekday") +
  guides(colour = guide_legend(override.aes = list(size=3.5))) +
  stat_smooth()
ggplot.monthly

# WEEKDAY VARIATIONS PLOT
dat.weekday <- subset(dat.daily, subset = (weekday.type == "weekday"))
ggplot.weekday<-ggplot(dat.weekday, aes(x=month.string, y = mean.daily.speed.kph))
ggplot.weekday+geom_boxplot(fill = "darkseagreen4")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_boxplot(aes(color = season))+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_jitter(alpha=0.5, aes(color=factor(season)), position = position_jitter(width = .2))
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+ geom_jitter(alpha=0.5, aes(color=season),
             position = position_jitter(width=0.1))+
ggtitle(paste('Weekday Variations by Month (',analysis_year,')',sep="")) +
theme(plot.title=element_text(size = 16, face = "bold", vjust=2)) +
scale_x_discrete(limits = all_months)

dat.weekend <- subset(dat.daily, subset = (weekday.type == "weekend"))
ggplot.weekend<-ggplot(dat.weekend, aes(x=month.string, y = mean.daily.speed.kph))
ggplot.weekend+geom_boxplot(fill = "darkseagreen4")+scale_x_discrete(limits = all_months)
ggplot.weekend+geom_boxplot(aes(color = season))+scale_x_discrete(limits = all_months)
ggplot.weekend+geom_jitter(alpha=0.5, aes(color=factor(season)), position = position_jitter(width = .2))
ggplot.weekend+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekend+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekend+geom_violin(alpha=0.5, color = "gray")+ geom_jitter(alpha=0.5, aes(color=season),
                                                                   position = position_jitter(width=0.1))+ 
  scale_x_discrete(limits = all_months)


#################################
# WORST PM PERIODS
#################################

for (i in avail_months){
  dat.temp <- read.csv(file = paste0("in/",analysis_year,"/hourly_",analysis_year,"_",sprintf("%02d",i),".csv"))
  dat.temp <- subset(dat.temp, subset = (dat.temp$hour >= 15 & dat.temp$hour <= 18))
  dat.temp$vol.length <- dat.temp$vol*dat.temp$length_m
  dat.temp <- dat.temp[c("tmc","length_m","day_only","weekday","hour","speedwtd","vol")]
  dat.temp$weight<-(dat.temp$vol*dat.temp$length_m)/mean((dat.temp$vol*dat.temp$length_m))
  
  lmer.hour <- lmer( speedwtd ~ 1
                     + (1|tmc:hour)
                     + (1|day_only), 
                     data = dat.temp,
                     weights = weight)
  
  filename = paste("out/models/pm/summary.lmer.hour.",i,".txt",sep='')
  write.table(tidy(lmer.hour), file = filename)
  
  filename = paste("out/models/pm/fixef.lmer.hour.",i,".txt",sep='')
  write(fixef(lmer.hour), file = filename)
  
  filename = paste("out/models/pm/ranef.lmer.hour.",i,".txt",sep='')
  write.table(ranef(lmer.hour)$day_only, file = filename)
}

fixef <- NULL
ranef <- NULL
for (i in avail_months) {
  temp <- read.table(paste("out/models/pm/fixef.lmer.hour.", i,  ".txt", sep=""), col.names = c("speed.mean.monthly"))
  fixef <- data.frame(rbind(fixef, temp))
  temp <- read.table(paste("out/models/pm/ranef.lmer.hour.", i,  ".txt", sep=""), col.names = c("date","speed.dif"), skip = 1)
  ranef <- data.frame(rbind(ranef, temp))
}

fixef$month <- avail_months

ranef$date <- as.Date(ranef$date)
ranef$month <- as.numeric(format(ranef$date, "%m"))
dat.daily <- merge(ranef, fixef, by = "month")
dat.daily$mean.daily.speed <- dat.daily$speed.mean.monthly + dat.daily$speed.dif

dat.date.lookup <- read.table("in/date_lookup_full.csv",header=TRUE, sep = ",")
dat.date.lookup <- subset(dat.date.lookup, subset = (year == analysis_year))
dat.date.lookup$date <- as.Date(as.character(dat.date.lookup$date), format = "%d-%b-%y")

dat.daily <- merge(dat.daily, dat.date.lookup, by =  "date")
rm(dat.date.lookup)

dat.daily <- rename(dat.daily, c("month.x"="month"))
months <- NULL
months$month <- avail_months
months$month.string <- all_months[avail_months]
dat.daily<-merge(dat.daily, months, by = "month")
rm(months)
dat.daily$month.y <- NULL

weekdays <- NULL
weekdays$weekday <- 1:7
weekdays$weekday.string <- c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
dat.daily <- merge(dat.daily, weekdays, by = "weekday")
rm(weekdays)

weekday.types <- NULL
weekday.types$weekday <- 1:7
weekday.types$weekday.type <- c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday.types)

dat.daily$mean.daily.speed.kph <- dat.daily$mean.daily.speed*1.60934
seasonality <- NULL
seasonality$month <- avail_months
seasonality$season <- seasons[avail_months]
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(seasonality)


###############################
# WORST PM PERIODS - PLOTS
###############################

# DAILY VARIATION PLOTS
ggplot.monthly <- ggplot(data = dat.daily, aes(x=day, y=mean.daily.speed.kph, colour = factor(weekday.type))) +
  ggtitle(paste('Daily Variations by Month (',analysis_year,')',sep="")) + 
  theme(plot.title=element_text(size = 16, face = "bold", vjust=2)) +
  geom_point() +
  facet_wrap(~month) +
  theme(legend.title = element_text(colour="black", size=12, face="bold")) +
  scale_color_discrete(name="Weekday") +
  guides(colour = guide_legend(override.aes = list(size=3.5))) +
  stat_smooth()
ggplot.monthly


# WEEKDAY VARIATIONS PLOT
dat.weekday <- subset(dat.daily, subset = (weekday.type == "weekday"))
ggplot.weekday <- ggplot(dat.weekday, aes(x=month.string, y = mean.daily.speed.kph))
ggplot.weekday + geom_boxplot(fill = "darkseagreen4")+scale_x_discrete(limits = all_months)
ggplot.weekday + geom_boxplot(aes(color = season))+scale_x_discrete(limits = all_months)
ggplot.weekday + geom_jitter(alpha=0.5, aes(color=factor(season)), position = position_jitter(width = .2))
ggplot.weekday + geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday + geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday + geom_violin(alpha=0.5, color = "gray")+ geom_jitter(alpha=0.5, aes(color=season),
                                                                   position = position_jitter(width=0.1))+
ggtitle(paste('Weekday Variations by Month (',analysis_year,')',sep="")) + 
theme(plot.title=element_text(size = 16, face = "bold", vjust=2)) +
scale_x_discrete(limits = all_months)


##################################
# SINGLE DAY ANALYSIS
##################################

#################
# WORST PERIODS #
#################
dat.daily <- read.table(paste("out/",analysis_year,"_daily/dat.daily.txt", sep = ""),header=TRUE)
worst_periods <- dat.daily$date[rank(dat.daily$mean.daily.speed.kph) <= 40]

dat.hour <- NULL
for (i in avail_months){
  dat.temp <- read.csv(file = paste0("in/",analysis_year,"/hourly_",analysis_year,"_",sprintf("%02d",i),".csv"))  
  dat.temp <- subset(dat.temp, subset = as.Date(dat.temp$day_only) %in% as.Date(worst_periods))
  dat.hour <- rbind(dat.hour, dat.temp)
}

dat.hour <- dat.hour[c("tmc","day_only","weekday","hour","speedwtd","vol","length_m")]
dat.hour$weight <- (dat.hour$vol*dat.hour$length_m)/mean((dat.hour$vol*dat.hour$length_m))

lmer.hour.slowest <- lmer( speedwtd ~ 1
                           + (1|tmc:hour)
                           + (1|day_only), 
                           data = dat.hour,
                           weights = weight)

tidy(lmer.hour.slowest)
fixef(lmer.hour.slowest)
ranef(lmer.hour.slowest)

# FURTHER PROCESSING
dat.daily <- ranef(lmer.hour.slowest)$day_only
colnames(dat.daily) <- c("speed.diff")
dat.daily$date <- as.Date(rownames(dat.daily))
dat.daily$avg.speed <- fixef(lmer.hour.slowest)
dat.daily$mean.daily.speed.kph <- (dat.daily$avg.speed + dat.daily$speed.diff)*1.60934

dat.date.lookup <- read.table("in/date_lookup_full.csv",header=TRUE, sep = ",")
dat.date.lookup <- subset(dat.date.lookup, subset = (year == analysis_year))
dat.date.lookup$date <- as.Date(as.character(dat.date.lookup$date), format = "%d-%b-%y")

dat.daily <- merge(dat.daily, dat.date.lookup, by = "date")
rm(dat.date.lookup)

months <- NULL
months$month <- avail_months
months$month.string <- all_months[avail_months]
dat.daily <- merge(dat.daily, months, by = "month")
rm(months)

weekday <- 1:7
weekday.string <- c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays <- data.frame(cbind(weekday, weekday.string))
dat.daily <- merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday.types <- NULL
weekday.types$weekday <- 1:7
weekday.types$weekday.type <- c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday.types)

seasonality <- NULL
seasonality$month <- avail_months
seasonality$season <- seasons[avail_months]
dat.daily <- merge(dat.daily, seasonality, by = "month")
rm(seasonality)

write.table(dat.daily[order(dat.daily$mean.daily.speed.kph),], file = paste("out/",analysis_year,"_daily/dat.daily.worst.",analysis_year,".txt", sep = ""))

#################
# BEST PERIODS #
#################
dat.daily <- read.table(paste("out/",analysis_year,"_daily/dat.daily.txt", sep = ""),header=TRUE)
best_periods <- dat.daily$date[rank(-dat.daily$mean.daily.speed.kph) <= 30]

dat.hour <- NULL
for (i in avail_months){
  dat.temp <- read.csv(file = paste0("in/",analysis_year,"/hourly_",analysis_year,"_",sprintf("%02d",i),".csv"))  
  dat.temp <- subset(dat.temp, subset = as.Date(dat.temp$day_only) %in% as.Date(best_periods))
  dat.hour <- rbind(dat.hour, dat.temp)
}

dat.hour <- dat.hour[c("tmc","day_only","weekday","hour","speedwtd","vol","length_m")]
dat.hour$weight <- (dat.hour$vol*dat.hour$length_m)/mean((dat.hour$vol*dat.hour$length_m))

lmer.hour.fastest <- lmer( speedwtd ~ 1
                           + (1|tmc:hour)
                           + (1|day_only), 
                           data = dat.hour,
                           weights = weight)

tidy(lmer.hour.fastest)
fixef(lmer.hour.fastest)
ranef(lmer.hour.fastest)

# FURTHER PROCESSING
dat.daily <- ranef(lmer.hour.fastest)$day_only
colnames(dat.daily) <- c("speed.diff")
dat.daily$date <- as.Date(rownames(dat.daily))
dat.daily$avg.speed <- fixef(lmer.hour.fastest)
dat.daily$mean.daily.speed.kph <- (dat.daily$avg.speed + dat.daily$speed.diff)*1.60934

dat.date.lookup <- read.table("in/date_lookup_full.csv",header=TRUE, sep = ",")
dat.date.lookup <- subset(dat.date.lookup, subset = (year == analysis_year))
dat.date.lookup$date <- as.Date(as.character(dat.date.lookup$date), format = "%d-%b-%y")

dat.daily <- merge(dat.daily, dat.date.lookup, by = "date")
rm(dat.date.lookup)

months <- NULL
months$month <- avail_months
months$month.string <- all_months[avail_months]
dat.daily <- merge(dat.daily, months, by = "month")
rm(months)

weekday <- 1:7
weekday.string <- c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays <- data.frame(cbind(weekday, weekday.string))
dat.daily <- merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday <- 1:7
weekday.type <- c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types <- data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

seasonality <- NULL
seasonality$month <- avail_months
seasonality$season <- seasons[avail_months]
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(seasonality)

write.table(dat.daily[order(-dat.daily$mean.daily.speed.kph),], 
            file = paste("out/",analysis_year,"_daily/dat.daily.best.",analysis_year,".txt", sep = ""))