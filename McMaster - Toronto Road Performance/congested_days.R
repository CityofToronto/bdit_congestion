library(RPostgreSQL)
library(plyr)
library(lme4)
library(broom)
library(ggplot2)
library(stringr)

analysis_year = 2014
all_months = c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


# IMPORT FROM POSTGRESQL
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "bigdata_ah", host = "137.15.155.38", port = 5432, user = "aharpal", password = "aakash")

# ESTIMATE A MULTI-LEVEL MODEL FOR EACH OF THE 12 MONTHS
for (i in 1:12) {
  strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
          FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
          WHERE EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id >= 1 AND TMC.id <= 20 AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
          " AND EXTRACT(MONTH FROM EHR.datetime_bin) = ",i, sep = '')
  dat.temp <- dbGetQuery(con, strSQL)

  vol.length<-dat.temp$volume*dat.temp$length_m
  dat.temp<-data.frame(cbind(dat.temp, vol.length))
  dat.hour<-ddply(dat.temp, . (id, length_m, day_only, weekday, hour), summarise,
                speed.wtd=sum(speed*vol.length)/sum(vol.length), vol=mean(volume))
  
  names(dat.hour)<-c("id", "length_m", "day_only", "weekday", "hour", "speed.wtd","vol")

  weight<-(dat.hour$vol*dat.hour$length_m)/mean((dat.hour$vol*dat.hour$length_m))
  dat.hour<-data.frame(cbind(dat.hour, weight))
  rm(vol.length, weight)
  
  lmer.hour <- lmer(speed.wtd~1+(1|id:hour)+(1|day_only), data = dat.hour,weights = weight)
  
  # summary(lmer.hour)
  # fixef(lmer.hour)
  # ranef(lmer.hour)$day_only
  # ranef(lmer.hour)

  setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash/out/models")
  
  summary.lmer.hour <- tidy(lmer.hour)
  
  filename = paste("summary.lmer.hour.",i,".txt",sep='')
  write.table(summary.lmer.hour, file = filename)
  
  filename = paste("fixef.lmer.hour.",i,".txt",sep='')
  write(fixef(lmer.hour), file = filename)
  
  ranef.lmer.hour<-ranef(lmer.hour)$day_only
  filename = paste("ranef.lmer.hour.",i,".txt",sep='')
  write.table(ranef.lmer.hour, file = filename)
  
  rm(ranef.lmer.hour, summary.lmer.hour, dat.hour, lmer.hour)
}

setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash/out/models")

# READ THE MEAN SPEEDS FOR EACH MONTH
fixef.1<-read.table("fixef.lmer.hour.1.txt", col.names = c("speed.mean.monthly"))
fixef.2<-read.table("fixef.lmer.hour.2.txt", col.names = c("speed.mean.monthly"))
fixef.3<-read.table("fixef.lmer.hour.3.txt", col.names = c("speed.mean.monthly"))
fixef.4<-read.table("fixef.lmer.hour.4.txt", col.names = c("speed.mean.monthly"))
fixef.5<-read.table("fixef.lmer.hour.5.txt", col.names = c("speed.mean.monthly"))
fixef.6<-read.table("fixef.lmer.hour.6.txt", col.names = c("speed.mean.monthly"))
fixef.7<-read.table("fixef.lmer.hour.7.txt", col.names = c("speed.mean.monthly"))
fixef.8<-read.table("fixef.lmer.hour.8.txt", col.names = c("speed.mean.monthly"))
fixef.9<-read.table("fixef.lmer.hour.9.txt", col.names = c("speed.mean.monthly"))
fixef.10<-read.table("fixef.lmer.hour.10.txt", col.names = c("speed.mean.monthly"))
fixef.11<-read.table("fixef.lmer.hour.11.txt", col.names = c("speed.mean.monthly"))
fixef.12<-read.table("fixef.lmer.hour.12.txt", col.names = c("speed.mean.monthly"))
fixef<-data.frame(rbind(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
                        fixef.9, fixef.10, fixef.11, fixef.12))
rm(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
                        fixef.9, fixef.10, fixef.11, fixef.12)
month<-1:12
fixef<-data.frame(cbind(fixef, month))

# READ THE SPEED DIFF EFFECTS FOR EACH DAY
ranef.1<-read.table("ranef.lmer.hour.1.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.2<-read.table("ranef.lmer.hour.2.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.3<-read.table("ranef.lmer.hour.3.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.4<-read.table("ranef.lmer.hour.4.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.5<-read.table("ranef.lmer.hour.5.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.6<-read.table("ranef.lmer.hour.6.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.7<-read.table("ranef.lmer.hour.7.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.8<-read.table("ranef.lmer.hour.8.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.9<-read.table("ranef.lmer.hour.9.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.10<-read.table("ranef.lmer.hour.10.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.11<-read.table("ranef.lmer.hour.11.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef.12<-read.table("ranef.lmer.hour.12.txt", col.names = c("day_only","speed.dif"), skip=1)
ranef<-data.frame(rbind(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
                       ranef.8, ranef.9, ranef.10, ranef.11, ranef.12))
rm(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
   ranef.8, ranef.9, ranef.10, ranef.11, ranef.12)



# CREATE DAILY MEAN SPEEDS ACROSS NETWORK
month<-as.numeric(substr(ranef$day_only,6,7))
ranef<-data.frame(cbind(ranef, month))
dat.daily<-merge(ranef, fixef, by = "month")
rm(month)
names(dat.daily)
mean.daily.speed<-dat.daily$speed.mean.monthly + dat.daily$speed.dif
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed))
rm(mean.daily.speed)
dat.daily$day_only = as.numeric(paste(as.numeric(substr(dat.daily$day_only,6,7)),str_sub(dat.daily$day_only,-2,-1),sep=''))


setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash")
dat.date.lookup<-read.table("in/date_lookup_full.csv",header=TRUE, sep = ",")
dat.date.lookup<-subset(dat.date.lookup, subset = (year == analysis_year))
day_only<-floor((dat.date.lookup$day.continuous.year-(analysis_year-2000))/100)
dat.date.lookup<-data.frame(cbind(day_only, dat.date.lookup$weekday))
names(dat.date.lookup)<-c("day_only", "weekday")
rm(day_only)

dat.daily<-merge(dat.daily, dat.date.lookup, by =  "day_only")
rm(dat.date.lookup)

dat.daily$day.of.month<-dat.daily$day_only-(100*dat.daily$month)

# ADDITIONAL FIELDS (MAKE MORE EFFICIENT LATER)
month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)
weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekday.string)
weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
rm(weekday, weekday.type)
dat.daily<-merge(dat.daily, weekday.types, by = "weekday")
mean.daily.speed.kph<-dat.daily$mean.daily.speed*1.60934
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed.kph))
rm(mean.daily.speed.kph)
month<-1:12
season<-c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
          "autumn", "autumn", "autumn")
seasonality<-data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)


#continue here.  WRITE DAT.DAILY
setwd("Y:/modeling/out/2014_daily/")
write.table(dat.daily, file = "dat.daily.14.txt")


#DAILY VARIATIONS PLOT

ggplot.monthly<-ggplot(data = dat.daily, aes(x=day.of.month, y=mean.daily.speed.kph, colour = factor(weekday.type)))+# color will color the road types individualy
  ggtitle('Daily Variations by Month (2014)')+theme(plot.title=element_text(size = 16, face = "bold", vjust=2))+
  geom_point()+ # add points
  facet_wrap(~month)+ #split graphs by time period
  theme(legend.title = element_text(colour="black", size=12, face="bold"))+
  scale_color_discrete(name="Weekday")+
  guides(colour = guide_legend(override.aes = list(size=3.5)))+
  stat_smooth()
ggplot.monthly




#WEEKDAY VARIATIONS PLOT
dat.weekday<-subset(dat.daily, subset = (weekday.type == "weekday"))
ggplot.weekday<-ggplot(dat.weekday, aes(x=month.string, y = mean.daily.speed.kph))
ggplot.weekday+geom_boxplot(fill = "darkseagreen4")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_boxplot(aes(color = season))+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_jitter(alpha=0.5, aes(color=factor(season)), position = position_jitter(width = .2))
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+ geom_jitter(alpha=0.5, aes(color=season),
             position = position_jitter(width=0.1))+
  ggtitle('Weekday Variations by Month (2014)')+theme(plot.title=element_text(size = 16, face = "bold", vjust=2))+
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


#THE FOLLOWING CODE CALCULATES THE WORST PM PERIODS
for (i in 1:12) {
  strSQL = paste("SELECT VOL.id, TMC.tmc, EHR.datetime_bin, DATE_TRUNC('day', EHR.datetime_bin) AS day_only, TMC.length_m, EHR.month, EHR.weekday, VOL.hour, EHR.obs, EHR.speed, VOL.volume
          FROM inrix.volumes VOL INNER JOIN inrix.ref_tmc TMC ON TMC.id = VOL.id INNER JOIN inrix.extract_hour EHR ON EHR.tmc = TMC.tmc
          WHERE VOL.hour >= 15 AND VOL.hour <= 18 AND EXTRACT(HOUR FROM EHR.datetime_bin) = VOL.hour AND TMC.id IN (1,2,3,4,5) AND EXTRACT(YEAR FROM EHR.datetime_bin) = ",analysis_year,
                 " AND EXTRACT(MONTH FROM EHR.datetime_bin) = ",i, sep = '')
  dat.temp <- dbGetQuery(con, strSQL)
  
  vol.length<-dat.temp$volume*dat.temp$length_m
  dat.temp<-data.frame(cbind(dat.temp, vol.length))
  dat.hour<-ddply(dat.temp, . (id, length_m, day_only, weekday, hour), summarise,
                  speed.wtd=sum(speed*vol.length)/sum(vol.length), vol=mean(volume))
  rm(dat.temp)
  
  names(dat.hour)<-c("id", "length_m", "day_only", "weekday", "hour", "speed.wtd","vol")
  
  weight<-(dat.hour$vol*dat.hour$length_m)/mean((dat.hour$vol*dat.hour$length_m))
  dat.hour<-data.frame(cbind(dat.hour, weight))
  rm(vol.length, weight)
  
  lmer.hour <- lmer(speed.wtd~1+(1|id:hour)+(1|day_only), data = dat.hour,weights = weight)
  
  setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash/out/models/pm")
  
  summary.lmer.hour <- tidy(lmer.hour)
  
  filename = paste("summary.lmer.hour.",i,".txt",sep='')
  write.table(summary.lmer.hour, file = filename)
  
  filename = paste("fixef.lmer.hour.",i,".txt",sep='')
  write(fixef(lmer.hour), file = filename)
  
  ranef.lmer.hour<-ranef(lmer.hour)$day_only
  filename = paste("ranef.lmer.hour.",i,".txt",sep='')
  write.table(ranef.lmer.hour, file = filename)
  
  rm(ranef.lmer.hour, summary.lmer.hour, dat.hour, lmer.hour)
}

setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash/out/models/pm")

fixef.1<-read.table("fixef.lmer.hour.1.txt", col.names = c("speed.mean.monthly"))
fixef.2<-read.table("fixef.lmer.hour.2.txt", col.names = c("speed.mean.monthly"))
fixef.3<-read.table("fixef.lmer.hour.3.txt", col.names = c("speed.mean.monthly"))
fixef.4<-read.table("fixef.lmer.hour.4.txt", col.names = c("speed.mean.monthly"))
fixef.5<-read.table("fixef.lmer.hour.5.txt", col.names = c("speed.mean.monthly"))
fixef.6<-read.table("fixef.lmer.hour.6.txt", col.names = c("speed.mean.monthly"))
fixef.7<-read.table("fixef.lmer.hour.7.txt", col.names = c("speed.mean.monthly"))
fixef.8<-read.table("fixef.lmer.hour.8.txt", col.names = c("speed.mean.monthly"))
fixef.9<-read.table("fixef.lmer.hour.9.txt", col.names = c("speed.mean.monthly"))
fixef.10<-read.table("fixef.lmer.hour.10.txt", col.names = c("speed.mean.monthly"))
fixef.11<-read.table("fixef.lmer.hour.11.txt", col.names = c("speed.mean.monthly"))
fixef.12<-read.table("fixef.lmer.hour.12.txt", col.names = c("speed.mean.monthly"))
fixef<-data.frame(rbind(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
                        fixef.9, fixef.10, fixef.11, fixef.12))
rm(fixef.1, fixef.2, fixef.3, fixef.4, fixef.5, fixef.6, fixef.7, fixef.8,
   fixef.9, fixef.10, fixef.11, fixef.12)
month<-1:12
fixef<-data.frame(cbind(fixef, month))

ranef.1<-read.table("ranef.lmer.hour.1.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.2<-read.table("ranef.lmer.hour.2.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.3<-read.table("ranef.lmer.hour.3.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.4<-read.table("ranef.lmer.hour.4.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.5<-read.table("ranef.lmer.hour.5.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.6<-read.table("ranef.lmer.hour.6.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.7<-read.table("ranef.lmer.hour.7.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.8<-read.table("ranef.lmer.hour.8.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.9<-read.table("ranef.lmer.hour.9.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.10<-read.table("ranef.lmer.hour.10.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.11<-read.table("ranef.lmer.hour.11.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef.12<-read.table("ranef.lmer.hour.12.txt", col.names = c("day_only","speed.dif"),skip=1)
ranef<-data.frame(rbind(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
                        ranef.8, ranef.9, ranef.10, ranef.11, ranef.12))
rm(ranef.1, ranef.2, ranef.3, ranef.4, ranef.5, ranef.6, ranef.7,
   ranef.8, ranef.9, ranef.10, ranef.11, ranef.12)

month<-as.numeric(substr(ranef$day_only,6,7))
ranef<-data.frame(cbind(ranef, month))
dat.daily<-merge(ranef, fixef, by = "month")
rm(month)
names(dat.daily)
mean.daily.speed<-dat.daily$speed.mean.monthly + dat.daily$speed.dif
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed))
rm(mean.daily.speed)
dat.daily$day_only = as.numeric(paste(as.numeric(substr(dat.daily$day_only,6,7)),str_sub(dat.daily$day_only,-2,-1),sep=''))

setwd("K:/tra/GM Office/Big Data Group/Work/Congestion Reporting - McMaster Update/R Code Testing - Aakash")
dat.date.lookup<-read.table("in/date_lookup_full.csv",header=TRUE, sep = ",") 
dat.date.lookup<-subset(dat.date.lookup, subset = (year == analysis_year))
day_only<-floor((dat.date.lookup$day.continuous.year-(analysis_year-2000))/100)
dat.date.lookup<-data.frame(cbind(day_only, dat.date.lookup$weekday))
names(dat.date.lookup)<-c("day_only", "weekday")
rm(day_only)

dat.daily<-merge(dat.daily, dat.date.lookup, by =  "day_only")
rm(dat.date.lookup)
dat.daily$day.of.month<-dat.daily$day_only-(100*dat.daily$month)

month<-1:12
months<-data.frame(cbind(month, all_months))
names(months) <- c("month", "month.string")
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)

weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekday.string)
 
weekday<-1:7
weekday.types<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.types))
rm(weekday)
dat.daily<-merge(dat.daily, weekday.types, by = "weekday")
mean.daily.speed.kph<-dat.daily$mean.daily.speed*1.60934
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed.kph))
rm(mean.daily.speed.kph)

month<-1:12
season<-c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
          "autumn", "autumn", "autumn")
seasonality<-data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)


#DAILY VARIATIONS PLOT

ggplot.monthly<-ggplot(data = dat.daily, aes(x=day.of.month, y=mean.daily.speed.kph, colour = factor(weekday.types)))+# color will color the road types individualy
  ggtitle('Daily Variations by Month (2014)')+theme(plot.title=element_text(size = 16, face = "bold", vjust=2))+
  geom_point()+ # add points
  facet_wrap(~month)+ #split graphs by time period
  theme(legend.title = element_text(colour="black", size=12, face="bold"))+
  scale_color_discrete(name="Weekday")+
  guides(colour = guide_legend(override.aes = list(size=3.5)))+
  stat_smooth()
ggplot.monthly




#WEEKDAY VARIATIONS PLOT

dat.weekday<-subset(dat.daily, subset = (weekday.types == "weekday"))
ggplot.weekday<-ggplot(dat.weekday, aes(x=month.string, y = mean.daily.speed.kph))
ggplot.weekday+geom_boxplot(fill = "darkseagreen4")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_boxplot(aes(color = season))+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_jitter(alpha=0.5, aes(color=factor(season)), position = position_jitter(width = .2))
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+scale_x_discrete(limits = all_months)
ggplot.weekday+geom_violin(alpha=0.5, color = "gray")+ geom_jitter(alpha=0.5, aes(color=season),
                                                                   position = position_jitter(width=0.1))+
  ggtitle('Weekday Variations by Month (2014)')+theme(plot.title=element_text(size = 16, face = "bold", vjust=2))+
  scale_x_discrete(limits = all_months)











################### BELOW HAS NOT BEEN TOUCHED BY AAKASH ###################

impute<-function(a, a.imute){
  ifelse(is.na(a),a.impute,a)
}







###########################old code
dat.2<-read.table("dat.time.out.txt",header=TRUE) 
dat.4<-read.table("tmc_lengths.txt", header = TRUE)
dat.4a<-read.table("trafficInrix_Join_Jan27b_Freeways.csv", header = TRUE, sep = ",")
dat.6<-read.table("trafficInrix_Join_Jan27b_tmc_newADD.csv", header = TRUE, sep = ",")


dat.7<-merge(dat.tmc.fid, dat.6, by.x="tmc",by.y="Tmc",all=FALSE)

dat.3<-merge(dat.7, dat.2, by.x="tmc",by.y="tmc",all=FALSE)
newADD.time.factor<-paste0(dat.3$newADD,"_",dat.3$time.factor)
dat.3<-data.frame(cbind(dat.3, newADD.time.factor))

rm(newADD.time.factor, volume)

dat.3<-merge(dat.3, dat.11.long, by.x = "newADD.time.factor", by.y = "newADD.time.factor", all = FALSE)
dat.3<-subset(dat.3,subset = (time.factor!="NA"))
ones<-rep(1,length = dim(dat.3)[1])
dat.3<-data.frame(cbind(dat.3, ones))
rm(ones)

dat.5<-merge(dat.3, dat.4, by.x="newADD.x",by.y="newADD",all=FALSE)

dat.uid<-read.table("torNetwork_UIDs_feb10.csv",header=TRUE, sep = ",") 
dat.uid<-merge(dat.uid, dat.tmc.fid, by.x = "tmc", by.y = "tmc", all = FALSE)
dat.5<-merge(dat.5, dat.uid, by.x="fid.tmc", by.y = "fid.tmc", all = FALSE)
dat.5<-merge(dat.5, dat.4a, by.x="newADD.x",by.y="newADD",all=FALSE)
direction<-dat.5$C_UID - dat.5$CorridorUID
dat.5<-data.frame(cbind(dat.5, direction))
rm(direction, temp)

###########################old code






#ALL THIS STUFF NEEDS TO BE UPDATED AS NEEDED.  
#EXTRACTING THE BACKGROUND DATA NEEDED TO DO EVERYTHING.  
#HERE I NEED TO MAKE SURE THAT THE INDICATORS ARE "CLEAN."  That way I can run a ddply with things and be done with it.






f.extract.corridor<-function(directory){
  setwd(directory)
  dat.uid<-read.table("torNetwork_UIDs_feb10.csv",header=TRUE, sep = ",") 
  
}

f.extract.corridor("Z:/modeling/toronto/in")
dat.uid<-merge(dat.uid, dat.tmc.fid, by.x = "tmc", by.y = "tmc", all = TRUE)






#setwd("Z:/modeling/toronto")
#dat.date.lookup.full<-read.table("in/date_lookup_full.csv",header=TRUE, sep = ",") 
#max(dat.date.lookup.full$week[dat.date.lookup.full$month==3&dat.date.lookup.full$day>25 & dat.date.lookup.full$year==2014])



#FOLLOWING IS THE ANALYSIS WHICH TAKES CARE OF COMPARING THE WORST PERIODS.
dat.temp<-subset(dat.extract.hour.14.2, subset = (day_only==106 | day_only == 110 | day_only == 110
                 | day_only == 125 | day_only ==  127 | day_only ==  205 | day_only ==  206
                 | day_only == 218 | day_only ==  302 | day_only ==  312 | day_only ==  313
                 | day_only == 328 | day_only ==  404 | day_only ==  408 | day_only ==  417 
                 | day_only ==  426 | day_only ==  429 | day_only ==  430 | day_only ==  503 
                 | day_only ==  515 | day_only ==  527 | day_only ==  606 | day_only ==  611
                 | day_only == 625 | day_only == 626 | day_only ==  724 | day_only ==  816 
                 | day_only ==  828 | day_only ==  902 | day_only ==  927 | day_only ==  928 
                 | day_only ==  1020 | day_only ==  1031 | day_only ==  1104 | day_only == 1108
                 | day_only == 1117 | day_only ==  1120 | day_only ==  1211 | day_only ==  1212 | day_only ==  1213
                 | day_only == 1217 ))
vol.length<-dat.temp$volume*dat.temp$Length_m
dat.temp<-data.frame(cbind(dat.temp, vol.length))
dat.hour<-ddply(dat.temp, . (newADD.x, day_only, weekday, hour), summarise,
                speed.wtd=sum(speed*vol.length)/sum(vol.length))
rm(dat.temp)
names(dat.hour)<-c("newADD", "day_only", "weekday", "hour", "speed.wtd")
newADD.time.factor<-paste0(dat.hour$newADD,"_",dat.hour$hour)
dat.hour<-data.frame(cbind(dat.hour, newADD.time.factor))
dat.hour<-merge(dat.hour, dat.14.long, by.x = "newADD", by.y = "newADD", all = FALSE)
dat.final.net1<-data.frame(cbind(dat.final.net$newADD, dat.final.net$Length_m))
names(dat.final.net1)<-c("newADD", "Length_m")
dat.hour<-merge(dat.hour, dat.final.net1, by.x = "newADD", by.y = "newADD", all = FALSE)
weight<-(dat.hour$volume*dat.hour$Length_m)/mean((dat.hour$volume*dat.hour$Length_m))
dat.hour<-data.frame(cbind(dat.hour, weight))
rm(vol.length, weight, newADD.time.factor)
lmer.hour.14.slowest<-lmer(speed.wtd~1+(1|newADD:hour)+(1|day_only), data = dat.hour,
                  weights = weight)
summary(lmer.hour.14.slowest)
fixef(lmer.hour.14.slowest)
ranef(lmer.hour.14.slowest)$day_only
#ranef(lmer.hour.14.slowest)
setwd("Y:/modeling/out")
summary.lmer.hour.14.slowest<-tidy(lmer.hour.14.slowest)
write.table(summary.lmer.hour.14.slowest, file = "summary.lmer.hour.14.slowest.txt")
write(fixef(lmer.hour.14.slowest), file = "fixef.lmer.hour.14.slowest.txt")
ranef.lmer.hour.14.slowest<-ranef(lmer.hour.14.slowest)$day_only
write.table(ranef.lmer.hour.14.slowest, file = "ranef.lmer.hour.14.slowest.txt")
rm(ranef.lmer.hour.14.slowest, summary.lmer.hour.14.slowest, dat.hour)
rm(lmer.hour.14.slowest)
save.image("Y:/modeling/rcode/congested_days_04-18-2015.RData")

#FOLLOWING IS THE ANALYSIS WHICH TAKES CARE OF COMPARING THE FASTEST PERIODS.
dat.temp<-subset(dat.extract.hour.14.2, subset = (day_only==101 | day_only == 112 | day_only ==  216 | day_only ==  217
  | day_only == 223 | day_only ==  320 | day_only ==  413 | day_only ==  418
  | day_only == 421 | day_only ==  504 | day_only == 517 | day_only ==  518
  | day_only == 519 | day_only ==  629 | day_only ==  630 | day_only ==  701
  | day_only == 714 | day_only ==  802 | day_only ==  803 | day_only ==  804
  | day_only == 901 | day_only ==  914 | day_only == 1005 | day_only ==  1013
  | day_only == 1103 | day_only ==  1111 | day_only ==  1207 | day_only ==  1221
  | day_only == 1225 | day_only ==  1226  ))
vol.length<-dat.temp$volume*dat.temp$Length_m
dat.temp<-data.frame(cbind(dat.temp, vol.length))
dat.hour<-ddply(dat.temp, . (newADD.x, day_only, weekday, hour), summarise,
                speed.wtd=sum(speed*vol.length)/sum(vol.length))
#rm(dat.temp)
names(dat.hour)<-c("newADD", "day_only", "weekday", "hour", "speed.wtd")
newADD.time.factor<-paste0(dat.hour$newADD,"_",dat.hour$hour)
dat.hour<-data.frame(cbind(dat.hour, newADD.time.factor))
dat.hour<-merge(dat.hour, dat.14.long, by.x = "newADD", by.y = "newADD", all = FALSE)
dat.final.net1<-data.frame(cbind(dat.final.net$newADD, dat.final.net$Length_m))
names(dat.final.net1)<-c("newADD", "Length_m")
dat.hour<-merge(dat.hour, dat.final.net1, by.x = "newADD", by.y = "newADD", all = FALSE)
weight<-(dat.hour$volume*dat.hour$Length_m)/mean((dat.hour$volume*dat.hour$Length_m))
dat.hour<-data.frame(cbind(dat.hour, weight))
rm(vol.length, weight, newADD.time.factor)
lmer.hour.14.fastest<-lmer(speed.wtd~1+(1|newADD:hour)+(1|day_only), data = dat.hour,
                           weights = weight)
summary(lmer.hour.14.fastest)
fixef(lmer.hour.14.fastest)
ranef(lmer.hour.14.fastest)$day_only
#ranef(lmer.hour.14.fastest)
setwd("Y:/modeling/out")
summary.lmer.hour.14.fastest<-tidy(lmer.hour.14.fastest)
write.table(summary.lmer.hour.14.fastest, file = "summary.lmer.hour.14.fastest.txt")
write(fixef(lmer.hour.14.fastest), file = "fixef.lmer.hour.14.fastest.txt")
ranef.lmer.hour.14.fastest<-ranef(lmer.hour.14.fastest)$day_only
write.table(ranef.lmer.hour.14.fastest, file = "ranef.lmer.hour.14.fastest.txt")
rm(ranef.lmer.hour.14.fastest, summary.lmer.hour.14.fastest, dat.hour)
rm(lmer.hour.14.fastest)
save.image("Y:/modeling/rcode/congested_days_05-05-2015_14.RData")







setwd("Y:/modeling/proc/congested_days/2014_slowest/")
fixef.slowest<-read.table("fixef.lmer.hour.14.slowest.txt", col.names = c("speed.mean.monthly"))

ranef.14.slowest<-read.table("ranef.lmer.hour.14.slowest.txt", col.names = c("day_only","speed.dif"))
month<-floor(ranef.14.slowest$day_only/100)
ranef.14.slowest<-data.frame(cbind(ranef.14.slowest, month))
speed.mean.monthly<-rep(fixef.slowest$speed.mean.monthly, length = (dim(ranef.14.slowest)[1]))
#names(fixef.slowest)<-c("speed.mean.monthly")
dat.daily<-cbind(ranef.14.slowest, fixef.slowest)
rm(month, speed.mean.monthly, ranef.14.slowest)
names(dat.daily)
mean.daily.speed<-dat.daily$speed.mean.monthly + dat.daily$speed.dif
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed))
rm(mean.daily.speed)

setwd("Z:/modeling/toronto")
dat.date.lookup<-read.table("in/date_lookup.csv",header=TRUE, sep = ",") 
dat.date.lookup<-subset(dat.date.lookup, subset = (year == 2014))
day_only<-floor((dat.date.lookup$day_only.year-14)/100)
dat.date.lookup<-data.frame(cbind(day_only, dat.date.lookup$weekday))
names(dat.date.lookup)<-c("day_only", "weekday")
rm(day_only)

dat.daily<-merge(dat.daily, dat.date.lookup, by =  "day_only")
rm(dat.date.lookup)

day.of.month<-dat.daily$day_only-(100*dat.daily$month)
dat.daily<-data.frame(cbind(dat.daily, day.of.month))
rm(day.of.month)
month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)
weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekday.string)
weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
rm(weekday, weekday.type)
dat.daily<-merge(dat.daily, weekday.types, by = "weekday")
mean.daily.speed.kph<-dat.daily$mean.daily.speed*1.60934
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed.kph))
rm(mean.daily.speed.kph)

month<-1:12
season<-c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
          "autumn", "autumn", "autumn")
seasonality<-data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)


#continue here.  
setwd("Y:/modeling/out")
write.table(dat.daily, file = "dat.daily.worst.2014.txt")





setwd("Y:/modeling/proc/congested_days/2014_fastest/")
fixef.fastest<-read.table("fixef.lmer.hour.14.fastest.txt", col.names = c("speed.mean.monthly"))

ranef.14.fastest<-read.table("ranef.lmer.hour.14.fastest.txt", col.names = c("day_only","speed.dif"))
month<-floor(ranef.14.fastest$day_only/100)
ranef.14.fastest<-data.frame(cbind(ranef.14.fastest, month))
speed.mean.monthly<-rep(fixef.fastest$speed.mean.monthly, length = (dim(ranef.14.fastest)[1]))
#names(fixef.fastest)<-c("speed.mean.monthly")
dat.daily<-cbind(ranef.14.fastest, fixef.fastest)
rm(month, speed.mean.monthly, ranef.14.fastest)
names(dat.daily)
mean.daily.speed<-dat.daily$speed.mean.monthly + dat.daily$speed.dif
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed))
rm(mean.daily.speed)

setwd("Z:/modeling/toronto")
dat.date.lookup<-read.table("in/date_lookup.csv",header=TRUE, sep = ",") 
dat.date.lookup<-subset(dat.date.lookup, subset = (year == 2014))
day_only<-floor((dat.date.lookup$day_only.year-14)/100)
dat.date.lookup<-data.frame(cbind(day_only, dat.date.lookup$weekday))
names(dat.date.lookup)<-c("day_only", "weekday")
rm(day_only)

dat.daily<-merge(dat.daily, dat.date.lookup, by =  "day_only")
rm(dat.date.lookup)

day.of.month<-dat.daily$day_only-(100*dat.daily$month)
dat.daily<-data.frame(cbind(dat.daily, day.of.month))
rm(day.of.month)
month<-1:12
month.string<-c("Jan", "Feb", "Mar", "Apr", "May", "June", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
months<-data.frame(cbind(month, month.string))
dat.daily<-merge(dat.daily, months, by = "month")
rm(month, months, month.string)
weekday<-1:7
weekday.string<-c("Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat")
weekdays<-data.frame(cbind(weekday, weekday.string))
dat.daily<-merge(dat.daily, weekdays, by = "weekday")
rm(weekday, weekday.string)
weekday<-1:7
weekday.type<-c("weekend", "weekday", "weekday", "weekday", "weekday", "weekday", "weekend")
weekday.types<-data.frame(cbind(weekday, weekday.type))
rm(weekday, weekday.type)
dat.daily<-merge(dat.daily, weekday.types, by = "weekday")
mean.daily.speed.kph<-dat.daily$mean.daily.speed*1.60934
dat.daily<-data.frame(cbind(dat.daily, mean.daily.speed.kph))
rm(mean.daily.speed.kph)

month<-1:12
season<-c("winter", "winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer",
          "autumn", "autumn", "autumn")
seasonality<-data.frame(cbind(month, season))
dat.daily<-merge(dat.daily, seasonality, by = "month")
rm(month, season, seasonality)


#continue here.  
setwd("Y:/modeling/out")
write.table(dat.daily, file = "dat.daily.fastest.2014.txt")


