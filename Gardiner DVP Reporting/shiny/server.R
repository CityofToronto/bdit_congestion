library(shiny)
library(lubridate)
library(ggplot2)
library(dplyr)
library(ggthemes)
library(viridis)

data <- read.csv("../data.csv")
data$datetime_bin <- as.POSIXct(data$datetime_bin)
ref <- read.csv("../segment_loc.csv")

# Define server logic required to plot various variables against mpg
shinyServer(function(input, output) {
  
  input_dir <- reactive({input$dir})
  date_start <- reactive({input$startdate})
  date_end <- reactive({input$enddate})
  dows <- reactive({input$dow})
  
  gplot <- reactive({
  
    hm_summary <- subset(data, subset = (dow %in% dows() 
                                         & direction == input_dir()
                                         & datetime_bin >= date_start()
                                         & datetime_bin < date_end())) %>%
      group_by(direction, segment, segment_name, bin_15) %>%
      summarise(mean_tt = mean(tt))
    
    hm_summary <- merge(hm_summary, ref, by = "segment_name")
    hm_summary$mean_spd <- (hm_summary$d*1.0) / (hm_summary$mean_tt*1.0) * 3.6
    
    gg <- ggplot(hm_summary, aes(x=bin_15+.125, y=segment, fill=mean_spd))
    gg <- gg + geom_raster()
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
    return(gg)
})
  
    output$test_plot <- renderPlot({gplot()})
})