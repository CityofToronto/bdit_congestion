library(shiny)

# Define UI for miles per gallon application
shinyUI(fluidPage(

  # Application title
  title = "Bluetooth Heat Maps",
  
  plotOutput("test_plot"),
  
  hr(),
  
  fluidRow(
    column(3,
      selectInput("dir", "Direction:",
                c("Eastbound/Northbound" = "EB/NB", 
                     "Southbound/Westbound" = "WB/SB"))
    ),
    column(3, offset = 1,
      dateInput("startdate", "Start Date:", value = "2016-09-01"),
      dateInput("enddate", "End Date:", value = "2016-11-30")
    ),
    column(3, offset = 1,
        checkboxGroupInput("dow", "Days of Week:",
                c("Sunday" = 1,
                  "Monday" = 2,
                  "Tuesday" = 3,
                  "Wednesday" = 4,
                  "Thursday" = 5,
                  "Friday" = 6,
                  "Saturday" = 7),
                selected = c(3,4,5)
    )
    )
  )


))