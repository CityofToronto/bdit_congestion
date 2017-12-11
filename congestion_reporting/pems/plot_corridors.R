library(plyr)
library(lubridate)

files <- list.files(path = "csv/", pattern="*.csv")
routes <- substr(files, 0, 3)
myfiles <- lapply(paste0("csv/",files), read.csv)

for (i in 1:length(routes)){
  colnames(myfiles[[i]]) <- c("datetime_bin","speed_kph")
  myfiles[[i]]$datetime_bin <- as.POSIXct(myfiles[[i]]$datetime_bin, format = "%m/%d/%Y %H:%M")
  myfiles[[i]]$route <- as.numeric(routes[i])
}

data <- ldply(myfiles, data.frame)
rm(files, i, myfiles, routes)

data$weekday <- wday(data$datetime_bin)
data$hr <- hour(data$datetime_bin)
data$inv_speed_kph <- 1/data$speed_kph
data <- data[data$datetime_bin >= '2017-05-16',]
data <- data[data$datetime_bin < '2017-06-16',]

summary <- aggregate(data$inv_speed_kph, by = data[c("route","weekday","hr")], FUN=mean)
summary$x <- 1/summary$x
write.csv(summary, "summary.csv")
