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