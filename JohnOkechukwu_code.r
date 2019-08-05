


#####Cluster/Environment setup
Created EMR cluster with SparkR
Spark allowed utilization of spark with Rstudio running on the cluster. This allowed for the use of the following code.
install.packages("SparkR")
install.packages("readr")
library(dplyr)
library(SparkR)
sc <- sparkR.session(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"),sparkPackages="com.databricks:spark-csv_2.10:1.4.0")
sparkR.session()


##### Data Cleanup 
collision<-read.csv(file="/home/john/NYPD_Motor_Vehicle_Collisions.csv", header = TRUE, sep = ",")
collision_df <- read.df("s3://sparkprojectbucket/NYPD_Motor_Vehicle_Collisions.csv","csv", header = "true", inferSchema = "true")
data.df<-as.DataFrame(collision)

collision = subset(collision, select = -c(LATITUDE,LONGITUDE,ZIP.CODE,LOCATION,ON.STREET.NAME,CROSS.STREET.NAME,OFF.STREET.NAME,CONTRIBUTING.FACTOR.VEHICLE.2,CONTRIBUTING.FACTOR.VEHICLE.3,CONTRIBUTING.FACTOR.VEHICLE.4,CONTRIBUTING.FACTOR.VEHICLE.5,VEHICLE.TYPE.CODE.2,VEHICLE.TYPE.CODE.3,VEHICLE.TYPE.CODE.4,VEHICLE.TYPE.CODE.5,NUMBER.OF.PEDESTRIANS.INJURED,NUMBER.OF.PEDESTRIANS.KILLED,NUMBER.OF.CYCLIST.INJURED,NUMBER.OF.CYCLIST.KILLED,NUMBER.OF.MOTORIST.INJURED,NUMBER.OF.MOTORIST.KILLED))
collision<-collision[!(collision$BOROUGH==""), ]                       	#removes blank boroughs
collision<-collision[!(collision$VEHICLE.TYPE.CODE.1==""), ]           	#removes blank first vehicle
collision<-collision[!(collision$CONTRIBUTING.FACTOR.VEHICLE.1==""), ] 	#removes blank contributing factor vehicle 1
collision$Year<-format(as.Date(collision$DATE,format="%m/%d/%Y"),"%Y")  #adds column for year 
collision$Month<-format(as.Date(collision$DATE,format="%m/%d/%Y"),"%b") #adds column for month
collision<-collision[!(collision$Year>2018 | collision$Year<2013),]    	#removes 2012 and 2019'
collision$Hour= format(as.POSIXct(collision$TIME,format="%H:%M"),"%H")


#### Data Analysis
#plots collisions by hour of day
barplot(table(as.numeric(collision$Hour)),xlab="Hour",ylab="Collisions",main="Collisions by Time of Day")
 
# plot collision by borough
barplot(table(collision$BOROUGH),xlab="Borough",ylab="Collisions",main="Collisions by Borough")


#plots collisions by Month
barplot(table(collision$Month),xlab="Month",ylab="Collisions",main="Collisions by Month")
 
library(ggplot2)
#plots number of people killed by hour
time_groups <- c(-Inf, 3, 7, 11, 15,19,24)
time_labels <- c("Late Night","Early Morning", "Morning Rush","Mid-Day","Afternoon", "Night")
collision$PartOfDay<-cut(as.numeric(collision$Hour),time_groups, labels = time_labels)
barplot(table(collision$PartOfDay),xlab="Part of Day",ylab="Collisions",main="Collisions by Part Of Day")
table(collision$PartOfDay,collision$Hour)
ggplot(collision, aes(x=Hour, y=NUMBER.OF.PERSONS.KILLED)) + geom_bar(stat="identity")
collision$DayOfWeek<-weekdays(as.Date(collision$ï..DATE,format="%m/%d/%Y"))
time_groups <- c(-Inf, 4, 8, 12, 16, 18,Inf)
time_labels <- c("Late Night","Early Morning", "Morning Rush","Mid-Day","Afternoon", "Night")
collision$PartOfDay<-cut(as.numeric(collision$Hour),time_groups, labels = time_labels)
 

collision$death[collision$NUMBER.OF.PERSONS.KILLED == 0] <-0
collision$death[collision$NUMBER.OF.PERSONS.KILLED > 0] <-1
collisionL <- glm(death ~ BOROUGH + PartOfDay, data = collision, family = "binomial")