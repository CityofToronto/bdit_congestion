library(RPostgreSQL)
library(lubridate)
library(ggplot2)
library(dplyr)
library(ggthemes)
library(viridis)
library(tables)
library(knitr)

########################
# CONNECT TO POSTGRESQL
########################
drv <- dbDriver("PostgreSQL")
con <- dbConnect()

####################
# RETRIEVE DATASETS
####################

strSQL = 
  paste0("SELECT * ",
         "FROM bluetooth.aggr_5min INNER JOIN bluetooth.ref_segments USING (analysis_id)",
         "WHERE datetime_bin >= '2016-09-01' AND datetime_bin < '2016-12-01' ",
         "AND segment_id IN (1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,22,46,47)")
data <- dbGetQuery(con, strSQL)
strSQL = 
  paste0("SELECT * ",
         "FROM bluetooth.ref_segments ",
         "WHERE segment_id IN (1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,22,46,47)")
segments <- dbGetQuery(con, strSQL)
ref <- read.csv("segment_loc.csv")

data$bin_15 <- hour(data$datetime_bin) + floor(minute(data$datetime_bin)/15)*25/100
data$bin_hr <- hour(data$datetime_bin)
data$dow <- wday(data$datetime_bin)
data$direction <- ifelse(data$startpointname > data$endpointname, "WB/SB", "EB/NB")
data$segment <- paste0(data$segment_name, ": ",data$start_crossstreet," (",data$start_road,
                       ") - ",data$end_crossstreet," (", data$end_road,")")

write.csv(data[c("analysis_id","datetime_bin","bin_15","dow","tt","direction","segment","segment_name")], "data.csv")

gex_eb <- c("A_B","B_C","C_D","D_E")
gex_wb <- c("B_A","C_B","D_C","E_D")
dvp_nb <- c("F_G","G_H","H_I","I_J","J_K")
dvp_sb <- c("G_F","H_G","I_H","J_I","K_J")

summary <- subset(data, subset = (dow >= 3 & dow <= 5)) %>%
  group_by(segment_name, segment, bin_15) %>%
  summarise(mean_tt = mean(tt))

summary$facility <- NA
summary$direction <- NA
summary[summary$segment_name %in% gex_eb,]$facility <- "Gardiner"
summary[summary$segment_name %in% gex_wb,]$facility <- "Gardiner"
summary[summary$segment_name %in% dvp_nb,]$facility <- "DVP"
summary[summary$segment_name %in% dvp_sb,]$facility <- "DVP"
summary[summary$segment_name %in% gex_eb,]$direction <- "EB"
summary[summary$segment_name %in% gex_wb,]$direction <- "WB"
summary[summary$segment_name %in% dvp_nb,]$direction <- "NB"
summary[summary$segment_name %in% dvp_sb,]$direction <- "SB"

# GARDINER
summary_gardiner <- subset(summary, subset = (facility == "Gardiner")) %>%
  group_by(bin_15, direction) %>%
  summarise(tt = sum(mean_tt))
d_gex_eb <- sum(ref[ref$segment_name %in% gex_eb,]$d)
d_gex_wb <- sum(ref[ref$segment_name %in% gex_wb,]$d)
summary_gardiner$spd <- 0
summary_gardiner[summary_gardiner$direction == "EB",]$spd <- (d_gex_eb*1.0) / (summary_gardiner[summary_gardiner$direction == "EB",]$tt)* 3.6
summary_gardiner[summary_gardiner$direction == "WB",]$spd <- (d_gex_wb*1.0) / (summary_gardiner[summary_gardiner$direction == "WB",]$tt)* 3.6

gex_eb_am <- 12/sum(1/subset(summary_gardiner, subset = (bin_15 >= 6.5 & bin_15 < 9.5 & direction == "EB"))$spd)
gex_eb_pm <- 12/sum(1/subset(summary_gardiner, subset = (bin_15 >= 16 & bin_15 < 19 & direction == "EB"))$spd)
gex_wb_am <- 12/sum(1/subset(summary_gardiner, subset = (bin_15 >= 6.5 & bin_15 < 9.5 & direction == "WB"))$spd)
gex_wb_pm <- 12/sum(1/subset(summary_gardiner, subset = (bin_15 >= 16 & bin_15 < 19 & direction == "WB"))$spd)

gg_gex <- ggplot(data = summary_gardiner, aes(x=bin_15, y = spd, group=direction, color = direction))
gg_gex <- gg_gex + geom_line(size=1)
gg_gex <- gg_gex + theme_pander(base_family = "arial")
gg_gex <- gg_gex + ggtitle("Average Speed on Gardiner Expressway (kph)", subtitle = "Tuesdays - Thursdays (September 1 - October 31, 2016)")
gg_gex <- gg_gex + theme(axis.title.y = element_blank())
gg_gex <- gg_gex + labs(x = "Time of Day")
gg_gex <- gg_gex + scale_x_continuous(breaks = scales::pretty_breaks(n=10))
gg_gex <- gg_gex + scale_y_continuous(breaks = scales::pretty_breaks(n=10), limits=c(0,110))
gg_gex <- gg_gex + theme(panel.grid.major = element_line(colour = "grey", linetype = "dotted"))
gg_gex <- gg_gex + theme(legend.position = c(0.5, 0.95), legend.title = element_blank(), legend.direction = "horizontal")
gg_gex <- gg_gex + annotate("rect",xmin = 6.5, xmax = 9.5, ymin = 0, ymax = Inf, fill = "grey", alpha = 0.2)
gg_gex <- gg_gex + annotate("text",x = 8, y = 10, label = "AM Peak")
gg_gex <- gg_gex + annotate("rect",xmin = 16, xmax = 19, ymin = 0, ymax = Inf, fill = "grey", alpha = 0.2)
gg_gex <- gg_gex + annotate("text",x = 17.5, y = 10, label = "PM Peak")
gg_gex <- gg_gex + annotate("text",x = 5.9, y = gex_wb_am, color = "#00BFC4",
                            label = paste0(round(gex_wb_am,0)," kph"), size = 2.5)
gg_gex <- gg_gex + annotate("segment",x = 6.5, xend=9.5, y = gex_wb_am, yend= gex_wb_am, 
                            colour = "#00BFC4", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_gex <- gg_gex + annotate("text",x = 5.9, y = gex_eb_am, color = "#F8766D",
                            label = paste0(round(gex_eb_am,0)," kph"), size = 2.5)
gg_gex <- gg_gex + annotate("segment",x = 6.5, xend=9.5, y = gex_eb_am, yend= gex_eb_am, 
                            colour = "#F8766D", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_gex <- gg_gex + annotate("text",x = 15.4, y = gex_wb_pm, color = "#00BFC4",
                            label = paste0(round(gex_wb_pm,0)," kph"), size = 2.5)
gg_gex <- gg_gex + annotate("segment",x = 16, xend=19, y = gex_wb_pm, yend= gex_wb_pm, 
                            colour = "#00BFC4", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_gex <- gg_gex + annotate("text",x = 15.4, y = gex_eb_pm, color = "#F8766D",
                            label = paste0(round(gex_eb_pm,0)," kph"), size = 2.5)
gg_gex <- gg_gex + annotate("segment",x = 16, xend=19, y = gex_eb_pm, yend= gex_eb_pm, 
                            colour = "#F8766D", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_gex


# DVP
summary_dvp <- subset(summary, subset = (facility == "DVP")) %>%
  group_by(bin_15, direction) %>%
  summarise(tt = sum(mean_tt))
d_dvp_nb <- sum(ref[ref$segment_name %in% dvp_nb,]$d)
d_dvp_sb <- sum(ref[ref$segment_name %in% dvp_sb,]$d)
summary_dvp$spd <- 0
summary_dvp[summary_dvp$direction == "NB",]$spd <- (d_dvp_nb*1.0) / (summary_dvp[summary_dvp$direction == "NB",]$tt)* 3.6
summary_dvp[summary_dvp$direction == "SB",]$spd <- (d_dvp_sb*1.0) / (summary_dvp[summary_dvp$direction == "SB",]$tt)* 3.6

dvp_nb_am <- 12/sum(1/subset(summary_dvp, subset = (bin_15 >= 6.5 & bin_15 < 9.5 & direction == "NB"))$spd)
dvp_nb_pm <- 12/sum(1/subset(summary_dvp, subset = (bin_15 >= 16 & bin_15 < 19 & direction == "NB"))$spd)
dvp_sb_am <- 12/sum(1/subset(summary_dvp, subset = (bin_15 >= 6.5 & bin_15 < 9.5 & direction == "SB"))$spd)
dvp_sb_pm <- 12/sum(1/subset(summary_dvp, subset = (bin_15 >= 16 & bin_15 < 19 & direction == "SB"))$spd)

gg_dvp <- ggplot(data = summary_dvp, aes(x=bin_15, y = spd, group=direction, color = direction))
gg_dvp <- gg_dvp + geom_line(size=1)
gg_dvp <- gg_dvp + theme_pander(base_family = "arial")
gg_dvp <- gg_dvp + ggtitle("Average Speed on Don Valley Parkway (kph)", subtitle = "Tuesdays - Thursdays (September 1 - October 31, 2016)")
gg_dvp <- gg_dvp + theme(axis.title.y = element_blank())
gg_dvp <- gg_dvp + labs(x = "Time of Day")
gg_dvp <- gg_dvp + scale_x_continuous(breaks = scales::pretty_breaks(n=10))
gg_dvp <- gg_dvp + scale_y_continuous(breaks = scales::pretty_breaks(n=10), limits=c(0,110))
gg_dvp <- gg_dvp + theme(panel.grid.major = element_line(colour = "grey", linetype = "dotted"))
gg_dvp <- gg_dvp + theme(legend.position = c(0.5, 0.95), legend.title = element_blank(), legend.direction = "horizontal")
gg_dvp <- gg_dvp + annotate("rect",xmin = 6.5, xmax = 9.5, ymin = 0, ymax = Inf, fill = "grey", alpha = 0.2)
gg_dvp <- gg_dvp + annotate("text",x = 8, y = 10, label = "AM Peak")
gg_dvp <- gg_dvp + annotate("rect",xmin = 16, xmax = 19, ymin = 0, ymax = Inf, fill = "grey", alpha = 0.2)
gg_dvp <- gg_dvp + annotate("text",x = 18.5, y = 10, label = "PM Peak")
gg_dvp <- gg_dvp + annotate("text",x = 5.9, y = dvp_sb_am, color = "#00BFC4",
                            label = paste0(round(dvp_sb_am,0)," kph"), size = 2.5)
gg_dvp <- gg_dvp + annotate("segment",x = 6.5, xend=9.5, y = dvp_sb_am, yend= dvp_sb_am, 
                            colour = "#00BFC4", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_dvp <- gg_dvp + annotate("text",x = 5.9, y = dvp_nb_am, color = "#F8766D",
                            label = paste0(round(dvp_nb_am,0)," kph"), size = 2.5)
gg_dvp <- gg_dvp + annotate("segment",x = 6.5, xend=9.5, y = dvp_nb_am, yend= dvp_nb_am, 
                            colour = "#F8766D", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_dvp <- gg_dvp + annotate("text",x = 15.4, y = dvp_sb_pm, color = "#00BFC4",
                            label = paste0(round(dvp_sb_pm,0)," kph"), size = 2.5)
gg_dvp <- gg_dvp + annotate("segment",x = 16, xend=19, y = dvp_sb_pm, yend= dvp_sb_pm, 
                            colour = "#00BFC4", linetype = "dotted", size = 0.5, alpha = 0.75)
gg_dvp <- gg_dvp + annotate("text",x = 15.4, y = dvp_nb_pm, color = "#F8766D",
                            label = paste0(round(dvp_nb_pm,0)," kph"), size = 2.5)
gg_dvp <- gg_dvp + annotate("segment",x = 16, xend=19, y = dvp_nb_pm, yend= dvp_nb_pm, 
                            colour = "#F8766D", linetype = "dotted", size = 0.5, alpha = 0.75)

ggsave(file="gex.svg", plot=gg_gex,width=9.5, height=3)
ggsave(file="dvp.svg", plot=gg_dvp,width=9.5, height=3)

# SUMMARY TABLE
segments$route <- paste0(segments$start_road, " ",segments$direction)
segments$route <- gsub("Gardiner EB/NB","Gardiner EB",segments$route)
segments$route <- gsub("DVP SB/WB","DVP SB",segments$route)

summary_table <- data.frame(segment_name=segments$segment_name,
                            route=segments$route,
                            begins_at=segments$start_crossstreet,
                            ends_at=segments$end_crossstreet,
                            stringsAsFactors = F)

summary$period <- NA
summary[summary$bin_15 >= 6.5 & summary$bin_15 < 9.5,]$period <- "AM Peak"
summary[summary$bin_15 >= 16 & summary$bin_15 < 19,]$period <- "PM Peak"
summary[summary$bin_15 >= 9.5 & summary$bin_15 < 16,]$period <- "Midday"
summary[summary$bin_15 >= 19 & summary$bin_15 < 22,]$period <- "Evening"

summary_table_data <- subset(summary, subset = !is.na(period)) %>%
  group_by(segment_name,period) %>%
  summarise(tt = mean(mean_tt))
summary_table_data <- merge(summary_table_data, ref, by="segment_name")
summary_table_data$spd <- (summary_table_data$d+0.0)/(summary_table_data$tt+0.0)*3.6
summary_table_data <- summary_table_data[,c("segment_name","period","d","spd")]
summary_table_data <- reshape(summary_table_data, timevar="period",idvar=c("segment_name","d"),direction="wide")

summary_table <- merge(summary_table, summary_table_data, by="segment_name")
summary_table <- summary_table[c(seq(1,9,by=2),seq(8,2,by=-2),seq(11,19,by=2),seq(20,10,by=-2)),]
row.names(summary_table) <- NULL
summary_table <- summary_table[,c("route","begins_at","ends_at","spd.AM Peak","spd.Midday","spd.PM Peak","spd.Evening")]
colnames(summary_table) <- c("Route","Begins At","Ends At","AM Peak","Midday","PM Peak","Evening")
summary_table[,4:7] <- round(summary_table[,4:7],0)
write.csv(summary_table,"table.csv")
#print(autoformat(xtable(summary_table)),type="latex")

# HEAT MAP
hm_summary <- subset(data, subset = (dow >= 3 & dow <= 5 & direction == "EB/NB")) %>%
  group_by(direction, segment, segment_name, bin_15) %>%
  summarise(mean_tt = mean(tt))

hm_summary <- merge(hm_summary, ref, by = "segment_name")
hm_summary$mean_spd <- (hm_summary$d*1.0) / (hm_summary$mean_tt*1.0) * 3.6

gg <- ggplot(hm_summary, aes(x=bin_15+.125, y=segment, fill=mean_spd))
gg <- gg + geom_tile(color="grey", size=0.01)
gg <- gg + scale_fill_viridis(option = "magma", direction = -1,name="mean speed", limits = c(0,120))
gg <- gg + coord_equal()
gg <- gg + labs(x = "Time of Day",y = NULL, title="Average Speed (kph) by Segment")
gg <- gg + theme_tufte(base_family = "arial")
gg <- gg + theme(legend.position="bottom")
gg <- gg + theme(plot.title=element_text(hjust=0, size=14))
gg <- gg + theme(legend.title=element_text(size=12))
gg <- gg + theme(legend.title.align=1)
gg <- gg + theme(legend.text=element_text(size=12))
gg <- gg + theme(legend.position="bottom")
gg <- gg + scale_x_continuous(breaks = scales::pretty_breaks(n=24))
gg <- gg + annotate("rect",xmin = 6.5, xmax = 9.5, ymin = 0.5, ymax = 10.5, color = "white", size = 0.5, alpha = 0)
gg <- gg + annotate("rect",xmin = 16, xmax = 19, ymin = 0.5, ymax =10.5, color = "white", size = 0.5, alpha = 0)