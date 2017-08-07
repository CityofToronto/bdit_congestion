library(RPostgreSQL)
library(tidyr)
library(dplyr)
library(plyr)
library(ggplot2)
library(ggthemes)


################################
# IMPORT FROM POSTGRESQL
################################
drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

# INPUTS
corridor_id <- 180
hh <- 17
seq_a <- 24
seq_b <- 23

# PLOT ONE
strSQL <-
  paste0( "SELECT datetime_bin::date, (CASE WHEN random()<=0.5 THEN MIN(total_tt) ELSE MAX(total_tt) END)/60.0 AS tt ",
          "FROM here_analysis.corridor_15min ",
          "WHERE corridor_id = ",corridor_id," AND EXTRACT(hour from datetime_bin) = ",hh,
          " GROUP BY datetime_bin::date ",
          "ORDER BY datetime_bin")
data <- dbGetQuery(con, strSQL)

strSQL <-
  paste0( "SELECT tt_ff/60.0 FROM here_analysis.corridor_ff WHERE corridor_id = ",corridor_id)
tt_ff <- as.numeric(dbGetQuery(con, strSQL))

strSQL <-
  paste0( "SELECT tt_avg/60.0 FROM here_analysis.corridor_summary WHERE corridor_id = ",corridor_id, " AND hh = ",hh, " AND year_bin IS NOT NULL")
tt_avg <- as.numeric(dbGetQuery(con, strSQL))

strSQL <-
  paste0( "SELECT tt_95/60.0 FROM here_analysis.corridor_summary WHERE corridor_id = ",corridor_id, " AND hh = ",hh, " AND year_bin IS NOT NULL")
tt_95 <- as.numeric(dbGetQuery(con, strSQL))

plot1 <- ggplot(data, aes(x = datetime_bin, y = tt)) +
  geom_hline(aes(yintercept = tt_ff), alpha = 0.4, size = 1.5, color = "grey") +
  geom_hline(aes(yintercept = tt_avg), alpha = 0.4, size = 1.5, color = "#66c2a5") +
  geom_hline(aes(yintercept = tt_95), alpha = 0.4, size = 1.5, color = "#fc8d62") +
  geom_point(color = "#8da0cb", size = 2.5) + 
  geom_line(alpha = 0.3, size = 1.5, color = "#8da0cb") +
  scale_x_date(date_breaks = '1 month', date_labels = "%b", limits = c(as.Date("2016-01-01","%Y-%m-%d"),as.Date("2016-12-31","%Y-%m-%d"))) +
  theme_few() +
  theme(text = element_text(size = 16)) +
  labs(y = NULL, x= NULL)
plot1

ggsave(plot1, file = "corridor_tt_example.jpg", units = "in", width = 10, height = 5, dpi = 500)

# PLOT TWO

strSQL <-
  paste0("SELECT ceiling(round(A.spd_avg,1)/5.0) as a_spd_bin, round(B.spd_avg,0) as b_spd ",
       "FROM here_analysis.corridor_links_15min A ",
       "INNER JOIN here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id) ",
       "WHERE corridor_id = ",corridor_id," AND A.seq = ",seq_a," AND B.seq = ",seq_b," AND A.estimated = FALSE AND B.estimated = FALSE ")
data_spd <- dbGetQuery(con,strSQL)
data_spd <- data_spd[data_spd$a_spd_bin %in% c(8,9,10),]
data_spd$a_spd_bin <- as.factor(data_spd$a_spd_bin)

spd_means <- ddply(data_spd, "a_spd_bin", summarise, spd.mean = mean(b_spd))
plot2 <- ggplot(data_spd, aes(x = b_spd)) +
  geom_density(alpha = 0.4, aes(group = a_spd_bin, fill = a_spd_bin)) +
  geom_vline(data=spd_means, aes(xintercept=spd.mean, color =  c("#7570b3","#1b9e77","#d95f02")),
             linetype="dashed", alpha = 0.4, size=1.2) +
  #geom_histogram(alpha = 0.5, aes(group = a_spd_bin, fill = a_spd_bin), position = "identity", binwidth = 1) +
  theme_few() +
  theme(text = element_text(size = 16)) +
  scale_x_continuous(limits = c(20,60)) +
  scale_y_continuous(limits = c(0,0.15)) +
  labs(y = NULL, x= "Average Speed (kph)") +
  scale_color_manual(values = c("#1b9e77","#d95f02","#7570b3")) +
  theme(legend.position="none")

plot2

ggsave(plot2, file = "speed_dist.jpg", units = "in", width = 10, height = 3.5, dpi = 500)

# PLOT 3

strSQL <-
  paste0("SELECT month_bin, corridor_id, hh, tti ",
    "FROM here_analysis.corridor_summary ",
    "WHERE hh IN (8,17) AND month_bin IS NOT NULL")
data_seas <- dbGetQuery(con, strSQL)
ind_means <- ddply(data_seas, c("month_bin","hh"),summarise, tti = mean(tti))
ind_means$hh <- as.factor(ind_means$hh)
plot3 <- ggplot(ind_means, aes(x = month_bin, y = tti, group = hh, color = hh)) +
  geom_line(size = 2, alpha = 0.7) +
  geom_point(size = 7) +
  theme_few() +
  theme(text = element_text(size = 16)) +
  scale_x_date(date_breaks = '1 month', date_labels = "%b", limits = c(as.Date("2015-12-25","%Y-%m-%d"),as.Date("2016-12-08","%Y-%m-%d"))) +
  coord_cartesian(ylim =c(1,2.5)) +
  labs(y = NULL, x= NULL)
plot3

strSQL <-
  paste0("SELECT month_bin, corridor_id, hh, bti ",
         "FROM here_analysis.corridor_summary ",
         "WHERE hh IN (8,17) AND month_bin IS NOT NULL")
data_seas <- dbGetQuery(con, strSQL)
ind_means <- ddply(data_seas, c("month_bin","hh"),summarise, bti = mean(bti))
ind_means$hh <- as.factor(ind_means$hh)
plot4 <- ggplot(ind_means, aes(x = month_bin, y = bti, group = hh, color = hh)) +
  geom_line(size = 2, alpha = 0.7) +
  geom_point(size = 7) +
  theme_few() +
  theme(text = element_text(size = 16)) +
  scale_x_date(date_breaks = '1 month', date_labels = "%b", limits = c(as.Date("2015-12-25","%Y-%m-%d"),as.Date("2016-12-08","%Y-%m-%d"))) +
  coord_cartesian(ylim =c(1,2)) +
  labs(y = NULL, x= NULL)
plot4
