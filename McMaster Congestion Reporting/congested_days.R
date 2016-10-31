library(RPostgreSQL)
library(plyr)
library(broom)
library(ggplot2)
library(stringr)
library(lme4)

analysis_year = 2014
all_months = c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

################################
# IMPORT FROM POSTGRESQL
################################
drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

################################
# MONTHLY MLM ESTIMATION
################################
for (i in 1:12) {
  strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
          FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
          WHERE EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id >= 1 AND TMC.id <= 20 AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
          " AND EXTRACT(MONTH FROM EHR.datetime_bin) = ",i, sep = '')
  
  dat.temp <- dbGetQuery(con, strSQL)

  dat.temp$vol.length <- dat.temp$volume*dat.temp$length_m
  dat.hour<-ddply( dat.temp, 
                   . (id, length_m, day_only, weekday, hour),
                   summarise,
                   speed.wtd = sum(speed * vol.length) / sum(vol.length),
                   vol=mean(volume))
  
  names(dat.hour) <- c("id", "length_m", "day_only", "weekday", "hour", "speed.wtd","vol")
  dat.hour$weight <- (dat.hour$vol*dat.hour$length_m) / mean((dat.hour$vol*dat.hour$length_m))
  lmer.hour <- lmer( speed.wtd ~ 1 +
                       (1|id:hour)
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
for (i in 1:12) {
  assign(paste("fixef.", i, sep=""), 
         read.table(paste("out/models/fixef.lmer.hour.", i,  ".txt", sep=""), col.names = c("speed.mean.monthly")))
}

fixef <- data.frame(rbind(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
                        fixef.9, fixef.10, fixef.11, fixef.12))
rm(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,fixef.9, fixef.10, fixef.11, fixef.12)
fixef$month <- 1:12

######################
# SPEED DIFF EFFECTS #
######################
for (i in 1:12) {
  assign(paste("ranef.", i, sep=""), 
         read.table(paste("out/models/ranef.lmer.hour.", i,  ".txt", sep=""), col.names = c("date","speed.dif"), skip = 1))
}

ranef <- data.frame(rbind(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
                       ranef.8, ranef.9, ranef.10, ranef.11, ranef.12))
rm(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
   ranef.8, ranef.9, ranef.10, ranef.11, ranef.12)
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
month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)
dat.daily$month.y <- NULL

weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

dat.daily$mean.daily.speed.kph <- dat.daily$mean.daily.speed*1.60934
month <- 1:12
season <- c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
          "autumn", "autumn", "autumn")
seasonality <- data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)

write.table(dat.daily, file = paste("out/",analysis_year,"_daily/dat.daily.txt", sep =""))


######################################
# MONTHLY MODEL ESTIMATION -  PLOTS
#####################################

# MONTHLY VARIATIONS PLOT
ggplot.monthly<-ggplot(data = dat.daily, aes(x=day, y=mean.daily.speed.kph, colour = factor(weekday.type))) +
  ggtitle(paste('Daily Variations by Month (',analysis_year,')',sep="") +
  theme(plot.title=element_text(size = 16, face = "bold", vjust=2)) +
  geom_point() +
  facet_wrap(~month) +
  theme(legend.title = element_text(colour="black", size=12, face="bold")) +
  scale_color_discrete(name="Weekday") +
  guides(colour = guide_legend(override.aes = list(size=3.5))) +
  stat_smooth()
ggplot.monthly

# WEEKDAY VARIATIONS PLOT
dat.weekday<-subset(dat.daily, subset = (weekday.type == "weekday"))
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

dat.weekend<-subset(dat.daily, subset = (weekday.type == "weekend"))
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

for (i in 1:12) {
  strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
          FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
          WHERE VOL.hour >= 15 AND VOL.hour <= 18 AND EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id IN (1,2,3,4,5) AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
                 " AND EXTRACT(MONTH FROM EHR.datetime_bin) = ",i, sep = '')
  dat.temp <- dbGetQuery(con, strSQL)
  
  dat.temp$vol.length<-dat.temp$volume*dat.temp$length_m
  dat.hour<-ddply(dat.temp, . (id, length_m, day_only, weekday, hour), summarise,
                  speed.wtd=sum(speed*vol.length)/sum(vol.length), vol=mean(volume))
  names(dat.hour)<-c("id", "length_m", "date", "weekday", "hour", "speed.wtd","vol")
  
  dat.hour$weight<-(dat.hour$vol*dat.hour$length_m)/mean((dat.hour$vol*dat.hour$length_m))
  
  lmer.hour <- lmer(speed.wtd~1+(1|id:hour)+(1|date), data = dat.hour,weights = weight)
  
  filename = paste("out/models/pm/summary.lmer.hour.",i,".txt",sep='')
  write.table(tidy(lmer.hour), file = filename)
  
  filename = paste("out/models/pm/fixef.lmer.hour.",i,".txt",sep='')
  write(fixef(lmer.hour), file = filename)
  
  filename = paste("out/models/pm/ranef.lmer.hour.",i,".txt",sep='')
  write.table(ranef(lmer.hour)$date, file = filename)

}

for (i in 1:12) {
  assign(paste("fixef.", i, sep=""), 
         read.table(paste("out/models/pm/fixef.lmer.hour.", i,  ".txt", sep=""), col.names = c("speed.mean.monthly")))
  assign(paste("ranef.", i, sep=""), 
         read.table(paste("out/models/pm/ranef.lmer.hour.", i,  ".txt", sep=""), col.names = c("date","speed.dif"), skip = 1))
}

fixef<-data.frame(rbind(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
                        fixef.9, fixef.10, fixef.11, fixef.12))
fixef$month<-1:12

ranef <- data.frame(rbind(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
                          ranef.8, ranef.9, ranef.10, ranef.11, ranef.12))
rm(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7,
   fixef.8, fixef.9, fixef.10, fixef.11, fixef.12)
rm(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
   ranef.8, ranef.9, ranef.10, ranef.11, ranef.12)

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
month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)
dat.daily$month.y <- NULL

weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

dat.daily$mean.daily.speed.kph <- dat.daily$mean.daily.speed*1.60934
month <- 1:12
season <- c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
            "autumn", "autumn", "autumn")
seasonality <- data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)


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

strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
          FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
               WHERE EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id >= 1 AND TMC.id <= 20 AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
               sep = '')

dat.temp <- dbGetQuery(con, strSQL)
dat.temp <- subset(dat.temp, subset = as.Date(dat.temp$day_only) %in% as.Date(worst_periods))

dat.temp$vol.length <- dat.temp$volume * dat.temp$length_m
tmc.char <- dat.temp[c("tmc","day_only","hour","volume","length_m")]
dat.hour <- ddply( dat.temp, 
                   . (tmc, day_only, weekday, hour), 
                   summarise,
                   speed.wtd = sum(speed*vol.length)/sum(vol.length))
names(dat.hour) <- c("tmc", "day_only", "weekday", "hour", "speed.wtd")

dat.hour <- merge(dat.hour, tmc.char, by = c("tmc","day_only","hour"))
rm(dat.temp, worst_periods)

dat.hour$weight <- (dat.hour$volume*dat.hour$length_m)/mean((dat.hour$volume*dat.hour$length_m))

lmer.hour.slowest <- lmer( speed.wtd ~ 1
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

month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)

weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

month <- 1:12
season <- c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
            "autumn", "autumn", "autumn")
seasonality <- data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)

write.table(dat.daily[order(dat.daily$mean.daily.speed.kph),], file = paste("out/",analysis_year,"_daily/dat.daily.worst.",analysis_year,".txt", sep = ""))

#################
# BEST PERIODS #
#################
dat.daily <- read.table(paste("out/",analysis_year,"_daily/dat.daily.txt", sep = ""),header=TRUE)
best_periods <- dat.daily$date[rank(-dat.daily$mean.daily.speed.kph) <= 30]

strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
               FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
               WHERE EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id >= 1 AND TMC.id <= 20 AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
               sep = '')

dat.temp <- dbGetQuery(con, strSQL)
dat.temp <- subset(dat.temp, subset = as.Date(dat.temp$day_only) %in% as.Date(best_periods))

dat.temp$vol.length <- dat.temp$volume * dat.temp$length_m
tmc.char <- dat.temp[c("tmc","day_only","hour","volume","length_m")]
dat.hour <- ddply( dat.temp, 
                   . (tmc, day_only, weekday, hour), 
                   summarise,
                   speed.wtd = sum(speed*vol.length)/sum(vol.length))
names(dat.hour) <- c("tmc", "day_only", "weekday", "hour", "speed.wtd")

dat.hour <- merge(dat.hour, tmc.char, by = c("tmc","day_only","hour"))
rm(dat.temp)

dat.hour$weight <- (dat.hour$volume*dat.hour$length_m) / mean((dat.hour$volume*dat.hour$length_m))

lmer.hour.fastest <- lmer( speed.wtd ~ 1
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

month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)

weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekdays, weekday.string)

weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
dat.daily <- merge(dat.daily, weekday.types, by = "weekday")
rm(weekday, weekday.type, weekday.types)

month <- 1:12
season <- c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
            "autumn", "autumn", "autumn")
seasonality <- data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)

write.table(dat.daily[order(-dat.daily$mean.daily.speed.kph),], 
            file = paste("out/",analysis_year,"_daily/dat.daily.best.",analysis_year,".txt", sep = ""))