library(RPostgreSQL)
library(ggplot2)
library(lubridate)
library(dplyr)
library(tidyr)
library(gridExtra)
library(extrafont)
library(statebins)
library(svglite)

# loadfonts(device = "win")

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

strSQL <-
  paste0("SELECT * FROM here_analysis.bt_ttr_monthly WHERE mth >= '2017-10-01' AND mth <= '2019-03-01' AND period_id IN (3,4,5)")
data <- dbGetQuery(con, strSQL)

data$period_id <- factor(data$period_id, levels = c(1,2,4,5,3), labels = c("AM Peak Hour", "PM Peak Hour","AM Peak Period","PM Peak Period", "Fri/Sat Night"))

data_summary <- data[data$mth == max(data$mth),]
data_summary$pct_change = paste0(round(data_summary$ttr_all-1,2)*100,"%",sep='')
data_summary$pct_change = ifelse(substr(data_summary$pct_change,1,1)=='-',data_summary$pct_change,paste0('+',data_summary$pct_change))

lims <- as.POSIXct(c("2017-10-01","2019-04-30"),format="%Y-%m-%d")
plot_breaks <- as.POSIXct(c("2017-10-01","2018-01-01","2018-04-01","2018-07-01","2018-10-01","2019-01-01","2019-03-01"),format="%Y-%m-%d")

colour_scale <- c("#660159")
font_default <- "Franklin Gothic Book"
x_labels = c("Oct\n2017","Jan\n2018","Apr","Jul","Oct","Jan\n2019","Mar")

ttr_plot <- ggplot(data = data, aes(x=mth)) +
        geom_line(aes(y = ttr_all - 1), colour = colour_scale, size = 2) +
        
        statebins:::geom_rrect(data = data_summary, mapping = aes(xmin = mth, xmax = mth + days(50),ymin=ttr_all-1.04,ymax=ttr_all-0.96),fill = "white", colour = colour_scale)+
  geom_text(data = data_summary, mapping = aes(x=mth+days(25), label = pct_change, 
                                               y = ttr_all-1), color = colour_scale, size = 3, hjust = 0.5, fontface = 2, family = font_default) +
        facet_grid(period_id ~ ., switch = "y") +
        theme_light() + 
        scale_x_datetime(breaks = plot_breaks, minor_breaks = plot_breaks, labels = x_labels, limits = lims) +
        scale_y_continuous(breaks = seq(-0.2,0.2,0.2), minor_breaks = seq(-0.2,0.2,0.2), limits = c(-0.2,0.2), labels=scales::percent_format(5L)) +
        theme(legend.position = "bottom", 
              axis.text = element_text(size = 10, family = font_default), 
              axis.title = element_text(size = 10, family = font_default, face = "bold"),
              axis.title.x = element_text(hjust = 0, family = font_default),
              axis.title.y = element_text(hjust = 1, family = font_default),
              strip.text.x = element_text(size = 10, family = font_default),
              strip.text.y = element_text(size = 10, family = font_default),
              plot.title = element_text(size = 10, family = font_default, face = "bold"),
              #plot.subtitle = element_text(size = 10, family = font_default),
              strip.background = element_rect(fill="#595959"),
              ) +
        labs(y = "Percentage Change in Travel Time", x = "Month", title = NULL) +
guides(fill=FALSE, colour = FALSE, alpha = FALSE)

ggsave("ttr_plot.svg",ttr_plot,width = 15.5, height = 12, units = "cm",dpi = 300)
