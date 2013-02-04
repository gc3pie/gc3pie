#==============================================================================
# UTILITIES FOR THE PROCESSING OF CONTINUOUS GPS RESULTS
# 
# This file contains functionality, only.
# Keep analysis scripts in separate files! 
# 
# V. Wirz, S. Gruber, UZH
#==============================================================================




#==============================================================================
# MISC STUFF
#==============================================================================
#construct method to concatenate strings with ampersand
"&" <- function(...) UseMethod("&") 
"&.default" <- .Primitive("&") 
"&.character" <- function(...) paste(...,sep="") 



#==============================================================================
# READ NODEPOSITIONS (.xls)
#==============================================================================
#
# Reads nodepositions file and returns in useful format
#
# INPUT
#   - file name of nodepositions Excel file, can be http address 
#     Example: "http://www.geo.uzh.ch/~stgruber/dirruhorn_nodepositions.xls"
#
# OUTPUT: data frame with:
#   - Position
#   - Label
#   - Name
#   - Comment
#   - Host.Name
#   - Sensor.ID
#   - Sensor.Type
#   - PowerBox.ID
#   - Masthead.Orientation
#   - Solar.Panel
#   - Mast.Height
#   - GPS.X
#   - GPS.Y
#   - GPS.Alt
#   - Device.ID.24.02.2012
#   - ... and more columns named Device.ID.[date]
#	
#==============================================================================
xs.positions.xls<-function(file=np.file) {
	#package for reading xls
	require("gdata")
	
	#read file
	np<-read.xls(file,skip=3,header=TRUE)
	#read date for device changes
	dev.dates<-names(read.xls(file,skip=1,header=TRUE))
	
	#fix names for device change and mast
	for (i in 1:length(dev.dates)) {
		if (substr(names(np)[i],1,9)=="Device.ID") {
			names(np)[i]<-substr(names(np)[i],1,9)&"."&substr(dev.dates[i],2,11)
		}
		if (names(np)[i]=="GPS.Mast") names(np)[i]<-"mast.o"
		if (names(np)[i]=="X.2")          names(np)[i]<-"mast.h"
	}
	
	np<- np[-1,]
	#change data types
	np$mast.o<-as.integer(as.character(np$mast.o))
	np$mast.h<- as.numeric(as.character(np$mast.h))
	np$Position<-as.numeric(np$Position)
	
	#drop lines without position
	np<-subset(np,is.na(Position)==FALSE)
	
	#return file contents
	return(np)
}#-----------------------------------------------------------------------------



#==============================================================================
# DEVICE CHANGES FROM NODEPOSITIONS
#==============================================================================
#
# Extracts device changes from nodeposition file.
#
# INPUT
#   - meta (output of xs.positions.xls)
#
# OUTPUT: data frame with:
#   - position                                        
#   - device_id
#   - dbeg        [POSIXct]
#   - dend        [POSIXct]
#   - duration
#	
#==============================================================================
xs.positions.changes<-function(meta) {
	last_date<-as.POSIXct(floor(as.numeric(Sys.time())/86400)*86400+86401,origin="1970-01-01")
	dev.change<-NULL
	#loop over positions
	for (pos in unique(meta$Position)) {
		MyPos<-subset(meta,Position==pos)
		ColValid<-NULL
		#loop over columns
		for (col in 17:ncol(MyPos)) {
			if (is.na(MyPos[col])==TRUE) next		
			ColValid<-rbind(ColValid,data.frame(position=pos,
							device_id=as.character(unlist(MyPos[col])),
							dbeg=as.POSIXct(names(MyPos)[col], "Device.ID.%d.%m.%Y", tz="UTC"),
					        dend=last_date)) #preliminary end date
		}
		#process one position further
		if (is.null(ColValid)==FALSE) {
			#sort position by time
			ColValid<-ColValid[with(ColValid, order(dbeg)), ]
			
			len<-nrow(ColValid)
			tmp<-NULL
			if (len > 1) {
				#drop rows that have no device change
				tmp<-ColValid[1,]
				for (row in 2:len) { 
					
					if (is.na(ColValid$device_id[row])==0 & is.na(ColValid$device_id[row-1])==0&
							ColValid$device_id[row]!=ColValid$device_id[row-1]) {
						tmp<-rbind(tmp,ColValid[row,])
					}
				}
				#make end date
				len<-nrow(tmp)
				for (row in 1:(len-1)) { 
					tmp$dend[row]<-tmp$dbeg[row+1]
				}
			}	
			dev.change<-rbind(dev.change,tmp)
		}
	}
	#exclude day of exchange
	dev.change$dbeg<-dev.change$dbeg+86401
	dev.change$dend<-dev.change$dend-1
			
	#duration
	dev.change$duration<-dev.change$dend-dev.change$dbeg

	#row names
	row.names(dev.change) <- NULL
    dev.change<-na.omit(dev.change)
	
	#return results
	return(dev.change)
}#-----------------------------------------------------------------------------




#==============================================================================
# CONVERT PERMASENSE UNIX TIME TO POXIXct (GENERIC FUNCTION)
#==============================================================================
#
# Generic function to read data from a GSN table.
# GSN syntax: http://sourceforge.net/apps/trac/gsn/wiki/web-interfacev1-server
#
# INPUT
#   - time [milliseconds past 1/1/1970]
#
# OUTPUT: 
#	- time [POSIXct]
#==============================================================================
unix2posix<-function(time) {
	return(as.POSIXct(time/1000,tz="UTC",origin="1970-01-01"))
}



#==============================================================================
# QUERY GSN (GENERIC FUNCTION)
#==============================================================================
#
# Generic function to read data from a GSN table.
# GSN syntax: http://sourceforge.net/apps/trac/gsn/wiki/web-interfacev1-server
#
# INPUT
#   - virtual_sensor (example: "dirruhorn_dozer_adccomdiff__mapped")
#   - position       (example: 1)
#   - fields   (default: "All", example: "position,device_id,generation_time")
#   - server   (default: "http://data.permasense.ch")
#   - time_beg (default: "01/01/2010+12:00")
#   - time_end (default: "01/01/2050+13:00")
#   - aggregation [h] (default: 0, i.e. no aggregation)
#
# OUTPUT: data frame of results, generation_time (etc.) converted to POSIXct if exists
#	
#==============================================================================
gsn.query<-function(virtual_sensor, position, fields="All", 
		            server="http://data.permasense.ch", aggregation=0, 
		            time_beg="01/01/2010+12:00", time_end="01/01/2050+13:00",
					verbose=TRUE) {
				
	#aggregation by time in hours
	if (aggregation > 0) {
		aggregation<-"&agg_function=avg&agg_unit=3600000&agg_period="&aggregation
	} else {
		aggregation<-""
	}		
				 
	#make request-------------------
	request<-server&"/multidata?"&
			#table/virtual sensor
			"vs[0]="&virtual_sensor&
			#fields
			"&field[0]="&fields&
			#aggregation
			aggregation&
			#select position
			"&c_vs[0]="&virtual_sensor&"&c_field[0]=position"&
			"&c_join[0]=and&c_min[0]="&position-1&"&c_max[0]="&position&
			#restrict time
			"&from="&time_beg&":00&to="&time_end&":00"&
			#sorting and format
			"&timeline=generation_time&download_format=csv&time_format=unix"	
	
	#read data---------------------
	if (verbose==TRUE) print("Sending request to GSN. Query string: '"&request&"'")
	#use try() to catch a connection that does not contain data
	data<-try(read.csv(request,skip=2,header=TRUE,sep=",",na.strings="null"),silent=TRUE)
	if (length(data)==1) {
		if (verbose==TRUE) print("No readable data returned from GSN. ")
		return(NULL)
	}
	if (verbose==TRUE) print("Received "&nrow(data)&" rows of data from GSN.") 
   
	#fix time-----------------------
	col<-which(names(data)=="generation_time")
	if (length(col)>0) data$generation_time<-unix2posix(data$generation_time)
	col<-which(names(data)=="start_date")
	if (length(col)>0) data$start_date<-unix2posix(data$start_date)
	col<-which(names(data)=="end_date")
	if (length(col)>0) data$end_date<-unix2posix(data$end_date)
	
	#fix position colname
	names(data)[1]<-"position"

	#make sure that only position is selected
	MyPosition<-position
	data<-subset(data,position==MyPosition)
	if (length(data$position)==0) data<-NULL

	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------




#==============================================================================
# AGGREGATE TIME SERIES (GENERIC FUNCTION)
#==============================================================================
#
# Generic function to aggregate time series to regular intervals
#
# INPUT
#   - data:  data frame that contains a POSIXct column called 'generation_time'
#   - width: with of averaging interval [sec], defaults to 3600
#   - funct: aggregation function [name], defaults to 'mean'
#
# OUTPUT: data frame of aggregated results
#         - generation_time [POSIXct] indicates end of averaging interval
#         - NA is ignored
#==============================================================================
xs.ts.aggregate<-function(data,width=3600,funct="mean") {
	#drop duplicates
	data<-unique(data)

	#generation_time column in right granularity
	data$generation_time<-as.POSIXct((floor(as.numeric(data$generation_time)/
						  width+1)*width),tz="UTC",origin="1970-01-01")

	#make aggregation			 
	data<-aggregate(data,list(data$generation_time),funct,na.rm=TRUE)
	data$generation_time<- data$Group.1
	
	#drop unused new column
	data<-subset(data,select=-Group.1)
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------





#==============================================================================
# READ GRABENGUFER CRACKMETER DATA FROM GSN
#==============================================================================
#
# Reads inclinometer data for one position and a time interval from GSN.
# Omitting start or end data will take all data since the
# beginning or until teh end of measuremnets at that position.
#
# INPUT
#   - time_beg (default: "01/01/2010+12:00")
#   - time_end (default: "01/01/2050+13:00")
#   - position (if omitted will default to 65 for Grabengufer)
#   - width:   with of averaging [sec], defaults to 3600      
#
# OUTPUT: data frame with inclinometer data, granularity: ~minutes
#   - generation_time [POSIXct]
#   - cm_length_mm [mm]
#	
#   Sorted by generation_time and rows with NA or duplicates are removed
#==============================================================================
xs.cm.gsn.gg<-function(position=65, width=3600,
		               time_beg="01/01/2010+12:00", time_end="01/01/2050+13:00") {

	#make request
	virtual_sensor<-"dirruhorn_dozer_adccomdiff__mapped"
	data<-gsn.query(virtual_sensor, position, time_beg=time_beg, time_end=time_end) 
	
	#clean data
	cm_length<-500 #length of crack meter in mm
	data$cm_length_mm<-data$payload_a0/(64000/cm_length)
	data<-subset(data,payload_sample_valid==1,select=c(generation_time,cm_length_mm))
	data<-unique(data) #remove duplicates
	
	#aggregate values
	data<-xs.ts.aggregate(data,width=width)
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------

#==============================================================================
# READ VAISALA  DATA FROM GSN
#==============================================================================
#
# Reads VAISALA data from GSN.
# Omitting start or end data will take all data since the
# beginning or until the end of measuremnets.
#
# INPUT
#   - time_beg (default: "01/01/2010+12:00")
#   - time_end (default: "01/01/2050+13:00")
#   - width:   with of averaging [sec], defaults to 3600  
#				original granularity of data: ~2 min
#
# OUTPUT: data frame with meteo (vaisala) data, 
#   - generation_time [POSIXct]
#   -  position"
#   -  device_id"
#   -  timestamp"             
#   -  wind_direction_minimum
#   -  wind_direction_average
#   -  wind_direction_maximum
#   -  wind_speed_minimum
#   -  wind_speed_average
#   -  wind_speed_maximum    
#   -  air_temperature
#   -  internal_temperature
#   -  relative_humidity     
#   -  air_pressure
#	
#   Sorted by generation_time and rows with NA or duplicates are removed
#==============================================================================
xs.vaisala.gsn<-function(width=3600,time_beg="01/12/2010+12:00", time_end="31/12/2012+13:00") {
	
	#make request
	virtual_sensor<-"dirruhorn_vaisalawxt520windpth"
	data<-gsn.query(virtual_sensor, position=13, 
			time_beg=time_beg, time_end=time_end)
	
	#aggregate values
	data<-xs.ts.aggregate(data,width=width)
	data<- data[order(data$generation_time), -3]
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------




#==============================================================================
# READ INCLINOMETER DATA FROM GSN
#==============================================================================
#
# Reads inclinometer data for one position and a time interval from GSN.
# Omitting start or end data will take all data since the
# beginning or until teh end of measuremnets at that position.
#
# INPUT
#   - time_beg
#   - time_end
#   - position
#
# OUTPUT: data frame with inclinometer data, granularity: ~minutes
#   - generation_time (POSIXct)
#   - position   (integer)
#   - device_id  (integer)
#   - x.DN       (inclinometer: x-direction [raw DN])
#   - y.DN       (inclinometer: y-direction [raw DN])
#   - T.C        (inclinometer: temperature [C])
#	
#   Data is sorted by generation_time and rows with NA are removed
#==============================================================================
xs.inc.gsn<-function(position, time_beg="01/01/2010+12:00", time_end="01/01/2050+13:00") {
	#make request
	virtual_sensor<-"dirruhorn_gps_logger__status"
	fields        <-"position,device_id,generation_time,temperature,inclinometer_x,inclinometer_y"
	data<-gsn.query(virtual_sensor, position, fields=fields, 
			        time_beg=time_beg, time_end="01/01/2050+13:00")
	if (is.null(data)==TRUE) return(NULL)
	
	#nice names
	names(data)<-c("position","device_id","T.C","x.DN",
			       "y.DN","generation_time")


	#remove NA and sort by time
	data<-na.omit(data)
	data<-data[with(data, order(generation_time)),]
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------


#==============================================================================
# READ GPS LOGGER CONFIGURATION DATA FROM GSN
#==============================================================================
#
# Reads logger config for one position and a time interval from GSN.
# Omitting start or end data will take all data since the
# beginning or until teh end of measuremnets at that position.
#
# INPUT
#   - time_beg
#   - time_end
#   - position
#
# OUTPUT: data frame with inclinometer data, granularity: ~minutes
#   - position     (integer)
#   - device_id    (integer)
#	- mast.o.start (mast orientation [deg])
#	- mast.o.end   (mast orientation [deg])
#   - date.start   (POSIXct)
#   - date.end     (POSIXct)
#	
#   Data is sorted by generation_time and rows with NA are removed
#==============================================================================
xs.con.gsn<-function(position, time_beg="01/01/2010+12:00", 
		                       time_end="01/01/2050+13:00") {	
	#make request
	virtual_sensor<-"dirruhorn_gps_logger__config"
	fields        <-"position,device_id,start_date,end_date,"&
			        "mast_orientation_start,mast_orientation_end"
	data<-gsn.query(virtual_sensor, position, fields=fields, 
			        time_beg=time_beg, time_end=time_end)
	if (is.null(data)==TRUE) return(NULL)
			
	#nice names
	names(data)<-c("position","device_id","date.start","date.end","mast.o.start","mast.o.end", "generation_time")
	
	#remove NA and sort by time
	data<-na.omit(data)
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------

#==============================================================================
# READ orientation of GPS-mast from csv file
#==============================================================================
#
# Reads mast-orientation (mast.o) from csv. file.
# The mast-orientation is measured manually in the field at the installation and 
# each time the logger is exchanged.
# INPUT
#   - position
#   - device-id
#   - start of period
#   - end of period
#
# OUTPUT: data frame with inclinometer data, granularity: ~minutes
#   - position     (integer)
#   - device_id    (integer)
#  - mast.o.start (mast orientation [deg])
#	- mast.o.end   (mast orientation [deg])
#   - date.start   (POSIXct)
#   - date.end     (POSIXct)
#	
#   Data is sorted by generation_time and rows with NA are removed
#==============================================================================
xs.con.csv<-function(position) {	
  #get data from csv-file
  conf.all<- read.csv(file=data.wd&"incl_config.csv")[,1:6]
  
  #convert to POSIXct
  conf.all$date.start<- as.POSIXct(as.character(conf.all$date.start), format= "%d.%m.%Y",tz="UTC")
  conf.all$date.end<- as.POSIXct(as.character(conf.all$date.end), format= "%d.%m.%Y",tz="UTC")
  
  #select subset
  conf<- subset(conf.all, position==pos)
  
  #return result as list of data frames
  return(conf)
}#----------------------------------------------------------------------------



#==============================================================================
# READ GPS PROCESSED DATA FROM GSN
#==============================================================================
#
# Reads GPS solutions for one position and a time interval from GSN.
# Omitting start or end data will take all data since the
# beginning or until teh end of measuremnets at that position.
#
# INPUT
#   - position
#   - subset   TRUE/FALSE (restrict output to what is needed for processing)
#   - time_beg
#   - time_end
#
# OUTPUT: data frame with daily GPS solutions
#   - position
#	- E.m      (E-cooridinate [m])
#	- N.m      (N-cooridinate [m])
#   - h.m      (h-cooridinate [m])
#	- sdE.m    (standard deviation of E-cooridinate [m])
#	- sdN.m    (standard deviation of N-cooridinate [m])
#   - sdh.m    (standard deviation of h-cooridinate [m])
#   - version
#   - processing_time
#   - generation_time
#	
#   Data is sorted by generation_time, per time, last version is cosen
#==============================================================================
xs.gps.pro.gsn<-function(position, subset=TRUE, 
		                 time_beg="01/01/2010+12:00", 
		                 time_end="01/01/2050+13:00") {
	
	#make request
	server        <-"http://data.permasense.ch"#"http://tik51x.ee.ethz.ch:22001" #needs ETH VPN
	virtual_sensor<-"dirruhorn_gps_differential__batch"
	fields        <-"All"
	data<-gsn.query(virtual_sensor, position, server=server, fields=fields, 
			        time_beg=time_beg, time_end="01/01/2050+13:00")
	
	if (is.null(data)==FALSE) {
	#select columns and rename, select last version for each time		
	if (subset==TRUE & is.null(data)==FALSE) {

		data<-subset(data,select=c(position,n,e,h,sd_n,sd_e,sd_h,sd_x,sd_y,sd_z,sd_xy,sd_yz,sd_zx,
						           version,generation_time,processing_time))
		names(data)<-c("position","N.m","E.m","h.m","sdN.m","sdE.m","sdh.m","sd_x","sd_y","sd_z",
					"cov_xy","cov_yz","cov_zx","version","generation_time","processing_time")
	}
		#select the largest version number for each day:
		# 1) sort by generation_time, then processing_time, then version
		# --> This means that for each day, the most recent processing will be used.
		#     If duplicates existe there, the highest Version number will be taken
			
		data<-data[with(data, order(generation_time,processing_time,version)),] 
		data$processing_time <- unix2posix(data$processing_time)
		#remove data with version < 0 (Test-versions)
		data<-data[data$version>0, ]
		
		# 2) remove duplicates of generation_time (backwards)
		ind<-duplicated(data$generation_time, fromLast=TRUE)
		data<-data[ind!=TRUE,]
		
		#fix sd- and cov-values (multiplication by 10 to get reasonable values)
 		   data$sdN.m <- data$sdN.m*10
 		   data$sdE.m <- data$sdE.m*10
 		   data$sdh.m <- data$sdh.m*10
		   #get cov-values for N,E,h
		   cov_NEh<-xs.gps.covar.loc(sd_x=data$sd_x,sd_y=data$sd_y,sd_z=data$sd_z,
				   cov_xy=data$cov_xy,cov_yz=data$cov_yz,cov_zx=data$cov_zx)
 		   data$cov_ne <- cov_NEh$sd_ne*10
 		   data$cov_nh <- cov_NEh$sd_nh*10
 		   data$cov_he <- cov_NEh$sd_he*10
		 
		 #drop unused columns
		 generation_time<-data$generation_time
		 data<-	subset(data,select=-c(sd_x,sd_y,sd_z,cov_xy,cov_yz,cov_zx,
						 version,generation_time,processing_time))
		 gps.data<- cbind(generation_time, data)
		 
		 #fix missing standard deviation
		 if (length(gps.data$position)==0) return(NULL)
		 gps.data[,6:11]<-gps.data[,6:11]+0.000001
	}
		 
	#return result as list of data frames
	return(gps.data)
}#----------------------------------------------------------------------------

#==============================================================================
# CALCULATE COVARIANCE-MATRIX IN SWISS COORDINATE SYSTEM
#
# transforms covariance-matrix in ecef to covariance-matrix in Swiss coord-syst. 
#
# Used Rotation-matrix see:http://de.wikipedia.org/wiki/Drehmatrix
#
#
#	
# INPUT: 
#		-covariance-matrix in ecef-syst
#
# RETURNS: dataframe with:
#		- -covariance-matrix in CH1903
#
#sd_x=data$sd_x;sd_y=data$sd_y;sd_z=data$sd_z;cov_xy=data$cov_xy;cov_yz=data$cov_yz;cov_zx=data$cov_zx
#
# Author: Vanessa Wirz
#==============================================================================
xs.gps.covar.loc<-function(sd_x,sd_y,sd_z,cov_xy,cov_yz,cov_zx){

		#lat&long of origin CH1903
		L<- 7.4386324175389165*pi/180   #
		B<-  46.9510827861504654*pi/180 + 3*pi/2 #
		
		#rotation matrices
		rL<- matrix(c(cos(L),-sin(L),0,sin(L),cos(L),0,0,0,1),nrow=3,byrow = 1) #lat around Z
		rB<-matrix(c(cos(B),0,sin(B),0,1,0,-sin(B),0,cos(B)),nrow=3,byrow = 1) #long around Y
		r.tot<- rB%*%rL #rotation matrix from ecef to CH1903
		
		cov_NEh<- NULL
	for(r in 1:length(sd_x)){
		#covar-matrix of X,y,z in ecef-system
		covXYZ<-	matrix(c(sd_x[r],cov_xy[r],cov_zx[r],  cov_xy[r],sd_y[r],cov_yz[r],
				cov_zx[r],cov_yz[r],sd_z[r]),byrow=T,ncol=3,nrow=3)
		#transformation in ch1903
		cov_m<- as.data.frame(covXYZ %*% r.tot)
		
		res<- data.frame(sd_ne=cov_m$V2[1],
						sd_nh=cov_m$V3[1],
						sd_he=cov_m$V3[2],
						sd_e=cov_m$V1[1],
						sd_n=cov_m$V2[2],
						sd_h=cov_m$V3[3])
		cov_NEh<- rbind(cov_NEh,res)
	}
	return(cov_NEh)

}#----------------------------------------------------------------------------	


#==============================================================================
# REMOVE OUTLIERS FROM DATA-VECTOR
#==============================================================================
#
# Removes outliers. Loops over a period of 30 days.	
# Calculates linear regression. The absolute difference between fitted value of 
# x and orignial value of x needs to be smaller than X*times the (absolute) 
# standard deviation of x. The default value of times is set to 50.
# Test based on data were done to get suitable values of times and the period 
# use for the regression.
#
#
# INPUT
#   - date
#   - x
#   - sd.x
#	- times (how many times the sd; threshold to decide if it ia an outlier)
#
# OUTPUT: index
#	- with TRUE for outliers
#	- 
# x=gps.data$N.m; sd.x<- gps.data$sdN.m; date<- gps.data$generation_time
# x=gps.data$E.m; sd.x<- gps.data$sdE.m; date<- gps.data$generation_time
# x= inc.daily$zen.deg_lm;sd.x= inc.daily$zen.deg_sd;date=inc.daily$generation_time;name="zen"
# x= inc.daily$azi.deg_lm;sd.x= inc.daily$azi.deg_sd;date=inc.daily$generation_time
# 
#==============================================================================
xs.remove.outlier<-function(x,sd.x,date, times=10,window=50,name){
	#allocate vectors
	diff<-rep(NA,length(x)); bad.ind<-diff
	#loop over periods of 3 days	
	for(i in seq(from=1, to=length(x), by=window+1) ){
		#linear regression with all data points
		if((length(x)-i)>window) ind<-i:(i+window)
		if((length(x)-i)<window) ind<-i:length(x)
		lmx<-lm(x[ind]~date[ind])
		#get fitted values
		fit<- fitted.values(lmx)
		diff[ind]<- abs(fit-x[ind])
	}
	#get index
	bad.ind<- diff > mean(diff,na.rm=T)*times#
	
	#fix na
	bad.ind[is.na(bad.ind)==1]<- FALSE

	#plot to test function
CTI<-0.393700787 #cm to inch by multiplication
pdf(file=plot.path&"outlier/pos-"&as.character(pos)&name&"_outlier.pdf",
		width=20*CTI, height=10*CTI)
ylim=c(0 , 1.2*max(max(diff,na.rm=T),mean(diff,na.rm=T)*times , abs(max(sd.x,na.rm=TRUE)*times)) )
plot(diff, x=date, col=bad.ind+1, ylim=ylim)
abline(h=mean(diff,na.rm=T)*times, col="blue")
abline(h=(max(abs(sd.x),na.rm=TRUE))*times,col="blue",lty=2)
dev.off()
	return(bad.ind)
}


#==============================================================================
# READ AGGREGATED GPS RAW DATA FROM GSN TO MAKE DATA OVERVIEW PLOT
#==============================================================================
#
# Reads daily aggregates of RAW GSN DATA. Makes individual daily queries for
# speed gain.
#
# IMPROVE FUNCTION BY FIRST QUERYING START/STOP DATE
#
# INPUT
#   - time_beg ("01/01/2010+12:00")
#   - time_end ("01/01/2012+12:00")
#   - position
#
# OUTPUT: data fame with daily averages of:
#   - position             (integer)
#   - device_id            (integer)
#   - gps_sats
#   - measurement_quality
#   - signal_strength
#   - generation_time      (POSIXct)
#   - aggregation_interval
#
#==============================================================================
xs.gps.raw.gsn<-function(position, time_beg, time_end) {
	#convert times to POSIXct
	format="%d/%m/%Y+%H:%M"
	time_beg<-as.POSIXct(time_beg, format=format)
	time_end<-as.POSIXct(time_end, format=format)		
	
	#query time steppng
	days <-floor((as.numeric(time_end)-as.numeric(time_beg))/86400)+1
	
	#query parameters
	virtual_sensor<-"dirruhorn_gps_raw__mapped"
	fields<-"position,device_id,generation_time,"&
			"gps_sats,measurement_quality,signal_strength"
	
	#make loop to prevent overly big query, drop some hours						   
	data<-NULL
	fb="%d/%m/%Y+00:00" #format for day start
	fe="%d/%m/%Y+23:59" #format for day end
	for (s in 0:days) {						   
		#make GSN time string from POSIXct
		now<-time_beg+86400*s #increment in days
		
#		count<-nrow(gsn.query(virtual_sensor, position, fields=fields[3], aggregation=0, 
#				time_beg=strftime(now,format=fb), 
#				time_end=strftime(now,format=fe),verbose=FALSE) )
		
#		if(count>0){
		#make request (sub-query)
		tmp<-gsn.query(virtual_sensor, position, fields=fields, aggregation=24, 
				time_beg=strftime(now,format=fb), 
				time_end=strftime(now,format=fe),verbose=FALSE)      
		#merge	   
		data<-rbind(data,tmp)
#		}
	}
	
	#sort data
	data<-data[with(data, order(generation_time)),]
	
	#Feedback
	print("GRP-RAW: Received "&nrow(data)&" rows of data from GSN")	
	
	#return result as list of data frames
	return(data)
}#----------------------------------------------------------------------------


#==============================================================================
# PREPROCESS INCLINOMETER DATA
#
# Converts to angles, gets noise level, aggregates to daily medians and 
# filters out days that do not have enough observations (threshold). Can
# handle several devices properly, but only ONE position. 
# A temperature correction is already applied in the inclinometer and 
# does not need to be performed.
#
# INPUT data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.h
#   - mast.o
#	- x.DN
#	- y.DN
#	- T.C
#
#   => sensitivity (default=32000) can be specified for conversion of DN
#      into angles. klen is the with of the boxcar filter used for computing
#      residuals as a basis for [x,y].deg.sd
#
# RETURNS: list of two data.frames. (1) "inc" with:
#	- generation_time
#	- position
#	- device_id
#	- x.deg.med   
#	- y.deg.med
#	- x.deg.sd   
#	- y.deg.sd
#	- count
#
#   and (2) "dev.change" with:
#	- position
#	- device_id
#	- dbeg   
#	- dend
#	- duration
#
# Author: Stephan Gruber
#==============================================================================
xs.inc.pre<-function(incl.raw,dev.change,threshold,sensitivity=32000,klen=289) {	
	#ensure only one position is processed
	if (is.null(incl.raw)==TRUE) return(NULL)
	if (length(unique(incl.raw$position))>1) return(NULL)
	
	#round generation_time to daily values
	orig<-as.numeric(ISOdatetime(1970,1,1,0,0,0,tz="UTC")) #in seconds
	incl.raw$day<-floor((as.numeric(incl.raw$generation_time)-orig)/86400)  
	
	#loop over device episodes
	incl.daily<-NULL
	for (epi in 1:length(dev.change$position)) {
		sub<-subset(incl.raw,generation_time>=dev.change$dbeg[epi] & 
						     generation_time<=dev.change$dend[epi] )
			 
		#test is enough measurements exist to process	 
		if (length(sub[,1]) > klen*2) {
			#get residuals ---------------------
			sub$x.res<-filter(sub$x.DN, rep(1, klen)/klen, sides=2,
			  		        method="convolution")
			sub$y.res<-filter(sub$y.DN, rep(1, klen)/klen, sides=2, 
					          method="convolution")		   
			#fix NAs
			len<-length(sub$x.res)
			hlen<-floor(klen/2)
			#beginning
			sub$x.res[1:hlen]<-sub$x.res[hlen+1]
			sub$y.res[1:hlen]<-sub$y.res[hlen+1]
			#end
			sub$x.res[(len-hlen):len]<-sub$x.res[len-hlen-1]
			sub$y.res[(len-hlen):len]<-sub$y.res[len-hlen-1]
   		    #actual residuals
			sub$x.res<-sub$x.res-sub$x.DN			
			sub$y.res<-sub$y.res-sub$y.DN			   
			#------------------------------------   
			
			#loop over days
			for (now.d in unique(sub$day)) {	
				#aggregate to daily median
				now<-subset(sub,day==now.d)
				now_n<-length(now[,1])
				now.day<-ISOdatetime(1970,1,1,12,0,0,tz="UTC")+
						             now.d*86400 #reference day to 12:00 noon	
							 
				res<-data.frame(generation_time=now.day,
							    position= dev.change$position[epi],
							    device_id=dev.change$device_id[epi],
								x.DN.med=median(now$x.DN),
								y.DN.med=median(now$y.DN),
								x.DN.sd=sd(now$x.res),
								y.DN.sd=sd(now$y.res),
								mast.o=median(now$mast.o),
								mast.h=median(now$mast.h),								
								count=now_n)
				#combine
				incl.daily<-rbind(incl.daily,res)

			}
		}
	}
	
	#exclude points with not enough observations
	incl.daily<-subset(incl.daily, count >= threshold)
	
	#compute angles and standard deviations
	DTOR<-pi/180
	offsetX = offsetY = 0
	incl.daily$x.deg.med<-asin( (incl.daily$x.DN.med - offsetX)
								/ sensitivity ) / DTOR
	incl.daily$y.deg.med<-asin( (incl.daily$y.DN.med - offsetY)
								/ sensitivity ) / DTOR

	#first part of sd cacluation
	incl.daily$x.deg.sd<-asin( (incl.daily$x.DN.med+incl.daily$y.DN.sd-offsetX)
								/ sensitivity ) / DTOR
	incl.daily$y.deg.sd<-asin( (incl.daily$y.DN.med+incl.daily$y.DN.sd-offsetY) 
								/ sensitivity ) / DTOR
	#second part. equivalent of sd in degrees
	incl.daily$x.deg.sd<-abs(incl.daily$x.deg.med-incl.daily$x.deg.sd)
	incl.daily$y.deg.sd<-abs(incl.daily$y.deg.med-incl.daily$y.deg.sd)
	
	#drop unused columns
	#incl.daily<-subset(incl.daily,select=-c(x.DN.med,y.DN.med,x.DN.sd,y.DN.sd))
	
	#return data
	return(incl.daily)
} #----------------------------------------------------------------------------

#==============================================================================
# PREPROCESS INCLINOMETER DATA --> GET DEGREES
#
# Changes DN values to degrees. Can handle several devices properly, but only 
# ONE position. 
# A temperature correction is already applied in the inclinometer and 
# does not need to be performed.
#
# INPUT data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.h
#   - mast.o
#	- x.DN
#	- y.DN
#
#   => sensitivity (default=32000) can be specified for conversion of DN
#      into angles. 
#
#
#
# RETURNS: list of two data.frames. (1) "inc" with:
#	- generation_time
#	- position
#	- device_id
#	- generation_time: POSIXct 
#	- position       : num  
#	- device_id      : Factor 
#	- mast.o         : num  
#	- mast.h         : num
#	- x.deg       : num  
#	- y.deg      : num
#
# Author: Vanessa Wirz, Stephan Gruber
#==============================================================================
xs.inc.deg<-function(inc.conf,dev.change,sensitivity=32000) {
	#set Offset for conversion into degrees
	DTOR<-pi/180
	offsetX = offsetY = 0
	
	#ensure only one position is processed
	if (is.null(inc.conf)==TRUE) return(NULL)
	if (length(unique(inc.conf$position))>1) return(NULL)
	
	#drop unused columns
	inc.deg<- inc.conf[, -c(3:5)]
	
	#compute angles
	inc.deg$x.deg<-asin( (inc.conf$x.DN - offsetX)
					/ sensitivity ) / DTOR
	inc.deg$y.deg<-asin( (inc.conf$y.DN - offsetY)
					/ sensitivity ) / DTOR
	
	return(inc.deg)
}


#==============================================================================
# PREPROCESS INCLINOMETER DATA
#
# Gets daily values. (Fits a linear regression per day.). Calculates standard 
# deviation, median and covariance per day.
# filters out days that do not have enough observations (threshold). Can
# handle several devices properly, but only ONE position. 
# A temperature correction is already applied in the inclinometer and 
# does not need to be performed.
#
# INPUT data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.h
#   - mast.o
#	- x.deg
#	- y.deg
# 
#
# RETURNS: data.frames with:
#	- generation_time
#	- position
#	- device_id
#	- generation_time: POSIXct 
#	- position       : num  
#	- device_id      : Factor
#	- x.deg.lm       : num  
#	- y.deg.lm       : num
#	- x.deg_med       : num
#	- y.deg_med       :num 
#	- x.deg_sd       : num 
#	- y.deg_sd       : num
#	- cov_xy.deg     : num
#	- mast.o         : num  
#	- mast.h         : num
#	- count          : int  
#
# Author: Vanessa Wirz, Stephan Gruber
#==============================================================================
xs.inc.pre.lm<-function(inc.glob,dev.change,threshold=20) {
	#set Offset for conversion into degrees
	DTOR<-pi/180
	offsetX = offsetY = 0
	
	#ensure only one position is processed
	if (is.null(inc.glob)==TRUE) return(NULL)
	if (length(unique(inc.glob$position))>1) return(NULL)
	
	#round generation_time to daily values
	orig<-as.numeric(ISOdatetime(1970,1,1,0,0,0,tz="UTC")) #in seconds
	inc.glob$day<-floor((as.numeric(inc.glob$generation_time)-orig)/86400)  
	
	#-------------------------------
	#loop over device episodes
	inc.daily<-NULL
	for (epi in 1:length(dev.change$position)) {
		sub<-subset(inc.glob,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
	 	
		#loop over days
		for (now.d in unique(sub$day)) {
			now<-subset(sub,day %in% now.d)
			now_n<-length(now[,1])
			now.day<-ISOdatetime(1970,1,1,12,0,0,tz="UTC")+
					now.d*86400 #reference day to 12:00 noon
			
			#linear regression to get daily values of x and y
			lmZen <- lm(now$zen.deg~now$generation_time)
				bx <- as.numeric(lmZen$coefficients[2])#coefficient
				ax <- as.numeric(lmZen$coefficients[1])#offset
				zen_lm <- ax+bx*as.numeric(now.day)

			lmAzi <- lm(now$azi.deg~now$generation_time)
				by <- as.numeric(lmAzi$coefficients[2])#coefficient
				ay <- as.numeric(lmAzi$coefficients[1])#offset
				azi_lm <- ay+by*as.numeric(now.day)
			
			#correction for temperature
#			lm.1<-lm(now$zen.deg~now$air_temperature,na.action=na.exclude)
#			zen.Tcorr1<-now$zen.deg-predict(lm.1)
#			zen.Tcorr2<-now$zen.deg-now$air_temperature*coef(lm.1)[2]
				
			#calculate covarianve
			cov_za<- cov(now$zen.deg,now$azi.deg)

			#make data row
			res <-data.frame(generation_time=now.day,
						position= dev.change$position[epi],
						device_id=dev.change$device_id[epi],
						zen.deg_lm=zen_lm,
						zen.deg_med=median(now$zen.deg),
						zen.deg_sd= sd(now$zen.deg),
						azi.deg_lm=azi_lm,
						azi.deg_med=median(now$azi.deg),
						azi.deg_sd=sd(now$azi.deg),
						cov_za.deg=cov_za, 
						mast.o=median(now$mast.o),
						mast.h=median(now$mast.h),
						count=now_n
#						,zen.Tcorr1=median(zen.Tcorr1)
#						,zen.Tcorr2=median(zen.Tcorr2)
				)
					
			inc.daily <- rbind(inc.daily,res)
		}# end of now.d for-loop
	}#end epi for-loop
	
return(inc.daily)
}

#==============================================================================
# PREPROCESS INCLINOMETER DATA
#
# Gets daily values. (at the moment: use median and boxcar-filter to test).  
# Calculates standard deviation, median and covariance per day.
# filters out days that do not have enough observations (threshold). Can
# handle several devices properly, but only ONE position. 
# A temperature correction is already applied in the inclinometer and 
# does not need to be performed.
#
# INPUT data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.h
#   - mast.o
#	- x.deg
#	- y.deg
# 
#
# RETURNS: data.frames with:
#	- generation_time
#	- position
#	- device_id
#	- generation_time: POSIXct 
#	- position       : num  
#	- device_id      : Factor
#	- x.deg.lm       : num  
#	- y.deg.lm       : num
#	- x.deg_med       : num
#	- y.deg_med       :num 
#	- x.deg_sd       : num 
#	- y.deg_sd       : num
#	- cov_xy.deg     : num
#	- mast.o         : num  
#	- mast.h         : num
#	- count          : int  
#
# Author: Vanessa Wirz, Stephan Gruber
#==============================================================================
xs.inc.pre.med<-function(inc.glob,dev.change,threshold=20, by=1) {
	#set Offset for conversion into degrees
	DTOR<-pi/180
	offsetX = offsetY = 0
	
	#ensure only one position is processed
	if (is.null(inc.glob)==TRUE) return(NULL)
	if (length(unique(inc.glob$position))>1) return(NULL)
	
	#round generation_time to daily values
	orig<-as.numeric(ISOdatetime(1970,1,1,0,0,0,tz="UTC")) #in seconds
	inc.glob$day<-floor((as.numeric(inc.glob$generation_time)-orig)/86400)  
	
	#-------------------------------
	#loop over device episodes
	inc.daily<-NULL
	for (epi in 1:length(dev.change$position)) {
		sub<-subset(inc.glob,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		
		#loop over days
		days=unique(sub$day); 
		for (now.d in days ){
			#for each day
			now<-subset(sub,day %in% now.d)
			now_n<-length(now[,1])
			now.day<-ISOdatetime(1970,1,1,12,0,0,tz="UTC")+
					now.d*86400 #reference day to 12:00 noon
			cov_za<- cov(now$zen.deg,now$azi.deg)#calculate covarianve
			
			#over period of days (given by "by")
			my.d<- seq(from=now.d-floor(by/2), to=now.d+floor(by/2))
			now2<-subset(sub,day %in% my.d)
			now_n2<-length(now2[,1])
			cov_za2<- cov(now2$zen.deg,now2$azi.deg)#calculate covarianve
			
			#make data row
			res <-data.frame(generation_time=now.day,
					position= dev.change$position[epi],
					device_id=dev.change$device_id[epi],
					mast.o=median(now$mast.o),
					mast.h=median(now$mast.h),
					zen.deg_med=median(now$zen.deg),
					zen.deg_sd= sd(now$zen.deg),
					azi.deg_med=median(now$azi.deg),
					azi.deg_sd=sd(now$azi.deg),
					cov_za.deg=cov_za, 
					count=now_n,
					zen.deg_med2=median(now2$zen.deg),
					zen.deg_sd2= sd(now2$zen.deg),
					azi.deg_med2=median(now2$azi.deg),
					azi.deg_sd2=sd(now2$azi.deg),
					cov_za.deg2=cov_za2,
					count2=now_n2
			)
			inc.daily <- rbind(inc.daily,res)
		}# end of now.d for-loop		
	}#end epi for-loop
	
	return(inc.daily)
}




#==============================================================================
# COMPLETE INCLINOMETER DATA WITH CONFIG VALUES
#==============================================================================
#
# Reads CONFIGURATION data for one position and adds it to inc.raw data frame.
# Linear interpolation between measured mast.o.
# Use eitheir only the measurement of mast.o when deployed (val2=FALSE) or  
# both measurements at deploying and read out (val2=TRUE).
# Meta ddata (mast.o) either comes from GSN-table config (csv==FALSE) or from 
# the csv-file (csv=TRUE).
#
# INPUT
#	- inc.raw (data.frame)
#   - time_beg
#   - time_end
#   - position
# 	in addition (logic): csv and val2
#
# OUTPUT: data frame with inclinometer data, granularity: ~minutes
#   - generation_time (POSIXct)
#   - position   (integer)
#   - device_id  (integer)
#   - x.DN       (inclinometer: x-direction [raw DN])
#   - y.DN       (inclinometer: y-direction [raw DN])
#   - T.C        (inclinometer: temperature [C])
#	
#   Data is sorted by generation_time and rows with NA are removed
#==============================================================================
#------------------------------------------------------------------------------
# Function for linear interpolation between aspects
#------------------------------------------------------------------------------
#start.o=210
#end.o=180
#xs.interp.aspect(start.o, end.o, length=10)
xs.interp.aspect <- function(start.o, end.o, length){
	#convert to radians
	DTOR<-pi/180
	start.rad <- 	start.o*DTOR
	end.rad <- 	end.o*DTOR
	#
	y=c(start.rad, rep(NA,length-2), end.rad)
	x=1:length
		tmp<- asin(approx(y=sin(y), x=x,xout=x)$y)/DTOR
		tmp2<- acos(approx(y=cos(y), x=x,xout=x)$y)/DTOR
	angle<- approx(y=y, x=x,xout=x)$y/DTOR
	
	if(start.o-end.o < -180 & start.o<180){angle=tmp %% 360}
	
	return(angle)
}
#REAL FUNCTION ---------------------------------------------------------------
xs.inc.config <- function(inc.raw, pos, mydev,csv=TRUE, val2=FALSE){
	
	#order inc.raw by generation_time
	inc.raw<- inc.raw[order(inc.raw$generation_time),]
	
  	if(csv==FALSE){
  	#get configuration data from GSN
	config<- xs.con.gsn(position=pos)
	config<- config[order(config$date.start),]}
  
 	 if(csv==TRUE){
  	#get mast.orientation from csv-file 
  	config<-xs.con.csv(pos)   
  	}
	
	if(val2==TRUE){
	#add mast.o to inc.raw data frame (on the days it is measured), use measurement of start and end 
	# of each device-period
	for(i in 1:nrow(config) ){
			time<- seq(from=config$date.start[i], to=config$date.end[i], length=10)
			interp<- xs.interp.aspect(config$mast.o.start[i], config$mast.o.end[i], length=length(time) )
			lm.o<- lm(interp~time)
			ind_period<- (inc.raw$generation_time>=config$date.start[i] &
					inc.raw$generation_time<=config$date.end[i])
			mast.o.interp<- coef(lm.o)[1]+coef(lm.o)[2]*as.numeric(inc.raw$generation_time[ind_period])
			inc.raw$mast.o[ind_period] <- mast.o.interp}
	}
	if(val2==FALSE){
		#add mast.o to inc.raw data frame, use measurement of deployment of device-id (only start
		# of each device-period
		for(i in 1:nrow(config) ){
			ind_period<- (inc.raw$generation_time>=config$date.start[i] &
						inc.raw$generation_time<=config$date.end[i])
			inc.raw$mast.o[ind_period] <-mean(config$mast.o.start[i],config$mast.o.end[i])
		}
	}
	
	
	#fix first/last value (!!!as long as config includes not all data)
	len<-length(inc.raw$mast.o) 
	if(is.na(inc.raw$mast.o[1])==1)inc.raw$mast.o[1] <- mydev$mast.o#first value
	if(is.na(inc.raw$mast.o[len])==1){#get last value
		# add last value (that is not NA to last value of mast.o
		mast.o.noNA <- inc.raw$mast.o[is.na(inc.raw$mast.o)==0]
		inc.raw$mast.o[len] <- mast.o.noNA[length(mast.o.noNA)]
	}
	#interpolate gaps
	inc.raw$mast.o<- approx(x=1:length(inc.raw$mast.o),y = as.numeric(inc.raw$mast.o),
			xout = 1:length(inc.raw$mast.o), method = "linear")$y
	
	#add mast.h to data-frame
	inc.raw$mast.h<- rep(mydev$mast.h, len)
	
	return(inc.raw)	
}

#==============================================================================
# READ INCLINOMETER DATA AND PREPROCESS FOR FURTHER USE
#==============================================================================
#
# Reads inclinometer data for one position and a time interval from GSN.
# Omitting start or end data will take all data since the
# beginning or until the end of measurements at that position.
# data are returned in degrees.
# --->if daily is set to TRUE. A linear regresssion is fitted per day to get the 
# daily value. Further the sd and the mean of this day is calculated (=> function:
# xs.inc.pre.lm)
# --->if daily is set to FALSE: data is not aggregated
#
# INPUT:
#   - time_beg
#   - time_end
#   - position
#	- dev.change
#	- mydev
#	- sd.mast.o
#	- daily
#
# RETURNS: data.frame with:
#	- generation_time
#	- position
#	- device_id
#   - mast.o -> linear interpoalted between measurements (made with device-change)
#   - mast.h
#	- x.deg.lm (daily value based on linear regression)   
#	- y.deg.lm (daily value based on linear regression)
#	- x.deg.mu (mean per day)
#	- y.deg.mu (mean per day)
#	- x.deg.sd (standard deviation per day)  
#	- y.deg.sd (standard deviation per day)
#	- count (number of measurements per day)
#
#   Data is sorted by generation_time and rows with NA are removed
#
# Author: Stephan Gruber, Vanessa Wirz	
#==============================================================================

#  REAL FUNCTION -------------------------------------------------------------
xs.inc.ready<-function(position, dev.change, mydev, sd.mast.o,daily=TRUE,
		               time_beg="01/01/2010+12:00", 
		               time_end="01/01/2050+13:00",by=1,
					   csv=TRUE, val2=FALSE) {
	#settings
	threshold  <-10    #min number of observations to return median data for a day
	sensitivity<-32000 #sensitivity of inclinometer
	
	#read from GSN
	inc.raw<-xs.inc.gsn(position, time_beg=time_beg, time_end=time_end)
	if (is.null(inc.raw)==TRUE) return(NULL)
	
	#complete inclino data
	inc.conf<- xs.inc.config(inc.raw, pos=pos, mydev, csv=TRUE, val2=FALSE)
# 	inc.conf$mast.o <- inc.conf$mast.o+10
	
	#convert to degrees
	inc.deg<- xs.inc.deg(inc.conf,dev.change,sensitivity=sensitivity)
	
	#calculate global coordinates & add to inc.deg
	glob<- xs.inc.loc2glob(inc.deg$x.deg, inc.deg$y.deg,angle=inc.deg$mast.o)
	inc.glob<- cbind(inc.deg, data.frame(zen.deg=glob$zen.deg, azi.deg=glob$azi.deg))
	
	#pre-process -> get daily values
	if(daily==1)inc.ready<- xs.inc.pre.med(inc.glob,dev.change,threshold=threshold,by)
	
	if(daily==0)inc.ready<- inc.glob[,-c(6,7)]

	#add sd of mast.o to data.frame
	inc.ready$sd.mast.o <- rep(sd.mast.o, length(inc.ready$mast.o))
	
	#return data
	return(inc.ready)
} #----------------------------------------------------------------------------



#==============================================================================
# COORDINATE TRANSFORMATION FROM LOCAL (TILTING GPS HEAD) TO GLOBAL COORDINATES
#
# Ich glaube die Mast-Orientierung zeigt in die Richtung der -X-Achse
# und alpha entspricht somit x.deg.
#
#
# Calculates the inclination (theta, from zenith) and aimut (phi, eastwards 
# from North) of the tilted GPS mast using a rotation-matrix. Can handle 
# multiple positions and devices.
#
# Convention of mast.o (discussion with B.Buchli, August 2012):
# The orientation screw in the mast head is in negative x-direction of
# the inclinometers. I.e. if the mast tilts towards the screw, x.DN (x.deg)
# become smaller (higher negative values).
#
#	
# INPUT: 
#	- x.deg (inclination in local x-direction, measured in tilting GPS)
#	- y.deg (inclination in local y-direction, measured in tilting GPS)
#	- angle (angle between North and the -X-direction of the vertical 
#            GPS mast [deg] eastwards from North.) 
#			The angle is meaured manualy (screw on the GPS-head)
#
# RETURNS: dataframe with:
#   - zen.deg
#   - azi.deg
#
# TESTING QUADRANTS: Run this:
# xs.inc.loc2glob(  2, 85,angle=0) #just >   0
# xs.inc.loc2glob( 85,  2,angle=0) #just <  90
# xs.inc.loc2glob( 85, -2,angle=0) #just >  90
# xs.inc.loc2glob(  2,-85,angle=0) #just < 180
# xs.inc.loc2glob( -2,-85,angle=0) #just > 180
# gps.angles.loc2glob(-85, -2,angle=0) #just < 270
# gps.angles.loc2glob(-85,  2,angle=0) #just > 270
# gps.angles.loc2glob( -2, 85,angle=0) #just < 360
#	
#x.deg=inc.deg$x.deg; y.deg=inc.deg$y.deg;angle=inc.deg$mast.o
#
# Author: Stefanie Gubler, Stephan Gruber, Vanessa Wirz
#==============================================================================
xs.inc.loc2glob<-function(x.deg, y.deg,angle=0) {
	#degrees to radians by multiplication
	DTOR<-pi/180
	
	#inclination measured in local rotating system at mast head
	alpha.rad <- x.deg*DTOR
	beta.rad  <- y.deg*DTOR
	
	#calculation
	theta.rad <- acos( sqrt (ifelse(cos(alpha.rad) ^2 + cos(beta.rad) ^ 2 -1<0,
							0, cos(alpha.rad) ^2 + cos(beta.rad) ^ 2 -1)   ))
#	phi.rad <- 1/2* ifelse((cos(alpha.rad) ^2 + cos(beta.rad) ^ 2 -2)==0,0,acos( (cos(alpha.rad) ^2 - 
#			   cos(beta.rad) ^ 2) / (cos(alpha.rad) ^2 + cos(beta.rad) ^ 2 -2)))
	
	#catch division by zero
	divisor<-(cos(alpha.rad)^2+cos(beta.rad)^2-2)
	divisor<-ifelse(divisor==0,-0.0000001,divisor)
	
	#catch division by zero
	divisor<-(cos(alpha.rad)^2+cos(beta.rad)^2-2)
	divisor<-ifelse(divisor==0,-0.0000001,divisor)
	
	#acos expression
	lim<-0.999999999
	phi.rad<-0.5* (ifelse( ((cos(alpha.rad)^2-cos(beta.rad)^2)/divisor)   >lim , 0,
						ifelse( ((cos(alpha.rad)^2-cos(beta.rad)^2)/divisor) <(-lim),pi,
								acos((cos(alpha.rad)^2-cos(beta.rad)^2)/divisor) ) ) )
	if(sum(is.na(phi.rad))==0 &&  sum(is.na(theta.rad))==0){print("no NAs produced")}
	
	#inclination and azimuth with respect to global coordinate system 
	#this system coincides with the local one when alpha=beta=0
	#this system is not aligned with an external grid or North
	theta <- theta.rad/DTOR #print(theta)
	phi.tmp   <- phi.rad   / DTOR; #print(phi)
	
	#quadrants are not right
	phi<-ifelse(x.deg > 0, 180-phi.tmp, phi.tmp)
	phi<-ifelse(y.deg < 0, 360-phi.tmp, phi.tmp)
	
	#rotate to align positive -x with North of external grid
	phi<-phi+angle
	phi<-phi %% 360  #scaling as modulo
	
	#return data
	return(list(zen.deg=theta,azi.deg=phi,phi.tmp=phi.tmp))
} #----------------------------------------------------------------------------





#==============================================================================
# ANGULAR DISTANCE BETWEEN TWO POINTS ON SPHERE
#
# Great-circle distance or orthodromic distanc as given by the
# Haverside function.
#
# INPUT
# 	- zen1 [deg]
#	- azi1 [deg]
#	- zen2 [deg]
#   - azi2 [deg]
#
# RETURNS: 
# 	- distance along great circle [deg]
#
# Author: Stephan Gruber
#==============================================================================
xs.dist.haversine<-function(zen1,azi1,zen2,azi2) {
	#degrees to radians by multiplication
	DTOR<-pi/180

	lat1<-(90-zen1)*DTOR
	lat2<-(90-zen2)*DTOR
	dlat<-abs(lat1-lat2)
	dlon<-abs(azi1*DTOR-azi2*DTOR)
	
	#Haversine (-->Wikipedia)
	d<-2*asin(sqrt(sin(dlat/2)^2+cos(lat1)*cos(lat2)*sin(dlon/2)^2))

	#return as degrees
	return(d/DTOR)
}#----------------------------------------------------------------------------



#==============================================================================
# RATE OF CHANGE (DISTANCE OR ROTATION)
#
# Fits a linear model that explains distance with time.
#
# INPUT data frame with:
# 	- time
#	- dist
#
# RETURNS: data frame with:
# 	- Estimate      (velocity: signal)
#   - Std. Error    (uncertainty: noise)
#
# Author: Stephan Gruber
#==============================================================================
xs.rate.linear<-function(data) {
	#fit linear model
	sum<-summary(lm(dist~0+time,data))
	
	#residual standard error of model
    #PREVIOUS: sd<-sum$sigma
	
	#Each coefficient in the model is a Gaussian (Normal) random variable. 
	#It is the estimate of the mean of the distribution of that random variable, 
	#and the standard error is the square root of the variance of that distribution.
	#http://stats.stackexchange.com/questions/5135/interpretation-of-rs-lm-output
	#STEPHAN: multiplied with sqrt(realizations) this number becomes stable. WHY?
	
	#proxy for noise
	sd<-as.numeric(unlist(sum$coefficients[2]))
	
	#coefficient
	co<-as.numeric(unlist(sum$coefficients[1]))
	
	#return coefficient and its standard error
	return(c(co,sd))
}#-----------------------------------------------------------------------------

xs.rate.inc<-function(data) {
	#fit linear models
	sum.x<-summary(lm(X~time,data))
	sum.y<-summary(lm(Y~time,data))

	#get intercepts [DN]
	in.x<-as.numeric(unlist(sum.x$coefficients[1,1]))
	in.y<-as.numeric(unlist(sum.y$coefficients[1,1]))
	
	#get coefficients [change in DN per day]
	co.x<-as.numeric(unlist(sum.x$coefficients[2,1]))
	co.y<-as.numeric(unlist(sum.y$coefficients[2,1]))
	
	#get standard errors [change in DN per day]
	sd.x<-as.numeric(unlist(sum.x$coefficients[2,2]))
	sd.y<-as.numeric(unlist(sum.y$coefficients[2,2]))
	
	#get standard errors [change in DN per day]
	pv.x<-as.numeric(unlist(sum.x$coefficients[2,4]))
	pv.y<-as.numeric(unlist(sum.y$coefficients[2,4]))
	
	#compute angles azi/inc [deg]
	p.in<-xs.inc.loc2glob(in.x,in.y)
	p.co<-xs.inc.loc2glob(in.x+co.x,in.y+co.y)
	p.sd<-xs.inc.loc2glob(in.x+sd.x,in.y+sd.y)
	
	#compute rotation and uncertainty of rotation [deg/day]
	rot   <-xs.dist.haversine(p.in$zen,p.in$azi,
			                  p.co$zen,p.co$azi)	
	rot.sd<-xs.dist.haversine(p.in$zen,p.in$azi,
			                  p.sd$zen,p.sd$azi)				  
					  
	#return coefficient and its standard error
	return(data.frame(rate=rot,rate.sd=rot.sd))
}#-----------------------------------------------------------------------------






#===========================================================================
# CALCULATE COORDINATES OF GPS FEET
#
# Correct GPS coordinates measured at to of mast for tilt to receive
# coordinated at the foot of the mast. Can handle multiple positions
# and devices.
#
# INPUT: data fame with these columns
#	- azi.deg.med (azimut of inclination, relative to N, eastwards [degrees])
#   - zen.deg.med (inclination, relative to zenith [degrees])
#	- azi.deg.sd (standard deviation [degrees])
#   - zen.deg.sd (standard deviation [degrees])
#	- E.m      (E-cooridinate [m])
#	- N.m      (N-cooridinate [m])
#   - h.m      (h-cooridinate [m])
#	- sdE.m    (standard deviation of E-cooridinate [m])
#	- sdN.m    (standard deviation of N-cooridinate [m])
#   - sdh.m    (standard deviation of h-cooridinate [m])
#	- mast.h   (mast heigth [m])
#
# RETURNS: same data frame, with new columns:
#   - dE (rotation-related displacement in E-direction [m])
#   - dN
#   - dh
#   - foot.E.m (E-coordinate after correction for rotation displacement [m])
#   - foot.N.m
#   - foot.h.m
#
#		E.m=sub$E.m;N.m=sub$N.m;h.m=sub$h.m;azi.deg=sub[,"azi.deg_"&agg]
#			zen.deg=sub[,"zen.deg_"&agg];mast.h=sub$mast.h
#
# AUTHORS: Vanessa Wirz, Stephan Gruber
#===========================================================================
xs.com.pre<-function(E.m,N.m,h.m,azi.deg,zen.deg,mast.h){
	#degrees to radians 
	DTOR<-pi/180
	
	#total displacement, in azimut direction [m]
	d.tot<-mast.h*sin(zen.deg*DTOR)
	#print("Mean d.tot for position "&pos&" is: "&mean(d.tot))
	
    #calc displacements in diverse directions [m]
	d.E<-d.tot* sin(azi.deg*DTOR) #dispalcement in E direction [m]
	d.N<-d.tot* cos(azi.deg*DTOR) #dispalcement in N direction [m]
	d.h<-mast.h*cos(zen.deg*DTOR)  #dispalcement in elevation [m]
			
	#coordinates/heigth of GPS-feet
	E.m<- E.m - d.E	#E-coordinate of GPS feet
	N.m<- N.m - d.N	#N-coordinate of GPS feet
	h.m<- h.m - d.h	#Elevation of GPS feet
	
	#make data frame
	res<-data.frame(x=E.m,y=N.m,z=h.m)
	
	#return
	return(res)
}#-----------------------------------------------------------------------------



#==============================================================================
# DISTANCE BETWEEN TWO CARTESIAN COORDINATES
#
# INPUT: two data frames, each with these columns:
# 	- Z     (for vertical distance only)
#   - X,Y   (for horizontal)
#   - X,Y,Z (for 3D)
#
# ==> keyword type ("horizontal", "vertical", "3D") determines method
#
# RETURNS: 
#   - dis (distance in original units)
#
#============================================================================== 
xs.dist.xyz<-function(t1,t2,type="horizontal") {
	#correct keyword
	parse=FALSE
	
	#horizontal distance
	if (toupper(type)=="HORIZONTAL") {
		#calculate differences
		dX<-t2$X-t1$X 
		dY<-t2$Y-t1$Y
		dis<-sqrt(dX^2 + dY^2)
	}
	
	#vertical distance
	if (toupper(type)=="VERTICAL") {
		dis<-t2$Z-t1$Z
	}
	
	#3D distance
	if (toupper(type)=="3D") {
		#calculate differences
		dX<-t2$X-t1$X 
		dY<-t2$Y-t1$Y
		dZ<-t2$Z-t1$Z
		dis<-sqrt(dX^2 + dY^2 + dZ^2)
	}
		
	#return distance
	return(dis)
}#-----------------------------------------------------------------------------
#calculates cumulative distance per device
xs.dist.xyz.per_dev<- function(data, dev.change){
  
  #loop over device-id
dist<-NULL; maxi.hor=0; maxi.3D=0; 
my.hor.dist<- NULL ;  my.3D.dist<- NULL
for (epi in 1:length(dev.change$position)) {
  #subset for this episode
  sub<-subset(data,generation_time>=dev.change$dbeg[epi] & 
                generation_time<dev.change$dend[epi] )
  sub<-sub[with(sub, order(generation_time)),] #sort by time
  len<-length(sub$position)

  #distance of this device-period
  my.hor.dist<- xs.dist.xyz(data.frame(X=sub$X[1],
             Y=sub$Y[1]), data.frame(X=sub$X,
             Y=sub$Y),type="horizontal") + maxi.hor
  if(length(my.hor.dist)==0) maxi.hor<- 0
  if(length(my.hor.dist)>0) maxi.hor<- my.hor.dist[len]
  
  my.3D.dist<- xs.dist.xyz(data.frame(X=sub$X[1],
                Y=sub$Y[1],Z=sub$Z[1]), data.frame(X=sub$X,
                Y=sub$Y, Z=sub$Z),type="3D") + maxi.3D
  if(length(my.3D.dist)==0) maxi.3D<- 0
  if(length(my.3D.dist)>0) maxi.3D<- my.3D.dist[len]

  #make data.frame
  my.dist<- data.frame(generation_time=sub$generation_time, dist_hor=my.hor.dist, dist_3d=my.3D.dist)
  dist<- rbind(dist, my.dist)   
}

return(dist)
}#-----------------------------------------------------------------------------

#==============================================================================
# TOTAL GREAT-CIRCLE DISTANCE
#
# Total rotation based on the great-circle distance (angle) between 
# a number of points and their first point.
#
# INPUT inclinometer data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.o
#   - zen.deg_lm
#   - azi.deg_lm
#  ==> dev.change
#
# RETURNS: vector of distances, length equals nrow of input   
#
# Author: Stephan Gruber, (Vanessa Wirz)
#==============================================================================
xs.inc.total<-function(inc, dev.change) {
	#make sure data exists
	if (is.null(inc)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	#ensure only one position is processed
	if (length(unique(inc$position))>1) return(NA)
	
	#loop over device episodes
	inc.tot<-NULL; generation_time<-NULL
	last<-0
	for (epi in 1:length(dev.change$position)) {
		#take subset for this period
		sub<-subset(inc,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		
		#beg work-------------------------------------------
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)	
		
		#angular distance based on both zen and azi
		
		now<-xs.dist.haversine(zen1=sub$zen.deg_med[is.na(sub$zen.deg_med)==0][1],
				azi1=sub$azi.deg_med[is.na(sub$azi.deg_med)==0][1],
				sub$zen.deg_med,   sub$azi.deg_med)			
		
		#combine
		inc.tot<-c(inc.tot,(now+last))
		last<-inc.tot[length(inc.tot)] #memorize to continue smoothly over device changes
    
    generation_time<- c(generation_time, sub$generation_time)
		#end work------------------------------------------
	}#episodes
	
  #make data.frame
  inc.dist<- data.frame(generation_time,inc.tot)
  
	#return result
	return(inc.dist)
} #----------------------------------------------------------------------------




#==============================================================================
# CALCULATE VELOCITY WITH MC-SIMULATIONS
#
# Uses multivariate normal distribution to estimate uncertainty and then  
# calculates regressions of the form: x= a + v * date
# Further calculates the Distance
# Returns the slope (v, velocity) and the offset (a)
#
#
#
# INPUT data frame (for 2D and inc: all gps or inc data for one device-id) with:
# 	- generation_time
#	- position
#	- device_id
#   - standard-deviation
#   - covariance
#   - X (gps-position E OR inclination in X-direction) 
#   - Y (gps-position N OR inclination in Y-direction) 
#   - Z (gps-elevation) 
#  ==> further parameters:
#	- realizations	(number of Monte-Carlo realizations)
#	- dimensions (character, 1D, 2D or 3D)
#	- for 2D: TYPE: "COM", "GPS"
#	- for 2D and inc: ind_now (index of actual data to use, subset of all data
#				in this "device-id" -episod
#
#
# RETURNS: new data frame with:
#	- v (slope of regression, velocity [m/day])		
#	- a (offset of regression)
#	
#	
# Author: Vanessa
#==============================================================================
#-- 1D version ----------------------------------------------------------------
xs.gps.MC.lm.1D<- function(date, E.m, sd_e, realizations=1000){
#needs library MASS
	library(MASS)
	
	#get length of data-vector
	len= length(E.m)	
	
	#make MC simulations
	mc<- NULL
	for(j in 1:realizations){
		
		#use normal distribution to estimate noise
		e<- NULL
		for(i in 1:len){
			uncert<- rnorm(n=1, mean=0, sd=sd_e[i])
			e[i]<- E.m[i]+uncert
		}
		
		#calculate linear regression and get coefficient and offset
		# X
		lme<- lm(e~date)
		v <- as.numeric(lme$coefficients[2])*86400
		a <- as.numeric(lme$coefficients[1])
		
		#make vector
		now <- cbind(a,v)
		
		#make data-frame
		mc<- rbind(mc,now)	
	}#end for j
	
	return(as.data.frame(mc))
}#-- END 1D

#-- 2D version ----------------------------------------------------------------
xs.gps.MC.lm.2D<- function(data, ind_now, realizations=10, TYPE="GPS",agg="med"){
	#needs library MASS
	library(MASS)
	
	#select subset of data& get length of data-vector
	sub<- data[ind_now,]
	len= dim(sub)[1]
	
	#make MC simulations
	mc<- NULL
	for(j in 1:realizations){	
		e<- NULL; n<-NULL;azi<- NULL;zen<- NULL;e.tmp<-NULL;n.tmp<-NULL
		
		if(TYPE=="GPS"){
		#use binomial distribution to estimate noise
		sigma=cbind(sub$sdE.m^2, sub$cov_ne, sub$cov_ne, sub$sdN.m^2)
		for(i in 1:len){
			
			my.sigma<-matrix(sigma[i,], ncol=2)
			#test if covar-matrix is positive definite
			if(round(det(my.sigma),digits=10)<=0 ){my.sigma[,1][2]=my.sigma[,2][1] =0}
			
			uncert<- mvrnorm(n=1, mu=c(0,0), Sigma=my.sigma)
			e[i]<- sub$E.m[i]+uncert[1]	
			n[i]<- sub$N.m[i]+uncert[2]	}
		}# end GPS
		
		
		if(TYPE=="COM"){
			#select daily values (aggregation-method)
			if(agg=="lm"){zen.d<-sub$zen.deg_lm;azi.d<-sub$azi.deg_lm}
			if(agg=="med"){zen.d<-sub$zen.deg_med;azi.d<-sub$azi.deg_med}
			if(agg=="med2"){zen.d<-sub$zen.deg_med2;azi.d<-sub$azi.deg_med2}
			
			#add uncertainty due to error in mast.o-measurements
			unc <- xs.interp.aspect(start.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), 
					end.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), length=10)
			time<- seq(from=range(data$generation_time)[1],
					to=range(data$generation_time)[2],length=10)
			lm.o<- lm(unc~time)
			unc.mast.o<- coef(lm.o)[1]+coef(lm.o)[2]*as.numeric(sub$generation_time)
			
			sigma.inc<- abs(cbind(sub[,"zen.deg_sd"&substr(agg,4,4)]^2,
					sub[,"cov_za.deg"&substr(agg,4,4)],sub[,"cov_za.deg"&substr(agg,4,4)],
				sub[,"azi.deg_sd"&substr(agg,4,4)]^2))
			sigma.gps=cbind(sub$sdE.m^2,sub$cov_ne,sub$cov_ne,sub$sdN.m^2)
			zen<-rep(NA,len); azi<-zen; e.tmp<-zen; n.tmp<-zen
			
			for(i in 1:len){
			
			my.sigma.inc<-matrix(sigma.inc[i,], ncol=2)
			my.sigma.gps<-matrix(sigma.gps[i,], ncol=2)
			#test if covar-matrix is positive definite
			if(round(det(my.sigma.inc),digits=10)<=0 ){my.sigma.inc[,1][2]=my.sigma.inc[,2][1] =0}
			if(round(det(my.sigma.gps),digits=10)<=0 ){my.sigma.gps[,1][2]=my.sigma.gps[,2][1] =0}
				
			#Incl
 			uncert<- mvrnorm(n=1, mu=c(0,0), Sigma=my.sigma.inc)
 			zen[i]<-zen.d[i] +uncert[1]
 			azi[i]<-azi.d[i] +uncert[2]
			
#			#print for eval
#			uncert.tot<- round(sub$mast.h*sin(uncert[1]*pi/180)*cos(uncert[2]*pi/180),digits=4)
#			print("dist due to uncertainty inc "&uncert.tot)
#			print("                                  ")
			
 			#gps
			uncert.gps<- mvrnorm(n=1, mu=c(0,0), Sigma=my.sigma.gps)
			e.tmp[i]<- sub$E.m[i]+uncert.gps[1]	
			n.tmp[i]<- sub$N.m[i]+uncert.gps[2]	
			}# enf for len
			
			#add uncertainty due to mast.o-measurements to azi
			azi<- azi+unc.mast.o
		
			#get coordinates at mast-foot
			foot<- xs.com.pre(E.m=e.tmp,N.m=n.tmp,h.m=sub$h.m,azi.deg=azi,
				zen.deg=zen,mast.h=sub$mast.h)
		
			e<-foot$x
			n<- foot$y
		}# end COM
		
		#calculate linear regression and get coefficient and offset
		# X
		lme<- lm(e~sub$generation_time)
		v_e <- as.numeric(lme$coefficients[2])*86400
		a_e <- as.numeric(lme$coefficients[1])
		#Y
		lmn<- lm(n~sub$generation_time)
		v_n <- as.numeric(lmn$coefficients[2])*86400
		a_n <- as.numeric(lmn$coefficients[1])
	
		#calculate distance (horizontal velocity) and the azimuth of movement
		v_en <- sqrt(v_e^2+v_n^2)
		azi_v <- atan2(v_e,v_n)*180/pi
		if(azi_v<0) azi_v<- 360+azi_v
		
		#make vector
		now <- data.frame(a_e,v_e,a_n,v_n,v_en,azi_v)
	
		#make data-frame
	mc<- rbind(mc,now)
	}#end j
	
	
	#make nice data-frame
	names(mc)<-c("a_e","v_e","a_n","v_n","v_en","azi_en")
	
	return(mc)	
}#--  end 2D
#-- 3D version ----------------------------------------------------------------
xs.gps.MC.lm.3D<- function(data, ind_now, realizations=10, TYPE="GPS",agg=NULL){
	#needs library MASS
	library(MASS)
	
	#select subset of data& get length of data-vector
	sub<- data[ind_now,]
	len= dim(sub)[1]
	
	#make MC simulations
	mc<- NULL
	for(j in 1:realizations){	
		e<- NULL; n<-NULL;h<-NULL;azi<- NULL;zen<- NULL;e.tmp<-NULL;n.tmp<-NULL;h.tmp<-NULL
		
		if(TYPE=="GPS"){
			#use binomial distribution to estimate noise
			sigma=cbind(sub$sdE.m^2, sub$cov_ne, sub$cov_ne,
					sub$cov_ne, sub$sdN.m^2,sub$cov_nh,
					sub$cov_he,sub$cov_nh,sub$sdh.m^2)
			for(i in 1:len){
				my.sigma.gps<-matrix(sigma[i,], ncol=3)
				#test if covar-matrix is positive definite
				if(round(det(my.sigma.gps),digits=10)<=0 ){my.sigma.gps[,1][2:3]=my.sigma.gps[,2][c(1,3)]=
							my.sigma.gps[,3][1:2] =0}
				
				uncert<- mvrnorm(n=1, mu=c(0,0,0), Sigma=my.sigma.gps)
				e[i]<- sub$E.m[i]+uncert[1]	
				n[i]<- sub$N.m[i]+uncert[2]	
				h[i]<- sub$h.m[i]+uncert[3]
			}
		}# end GPS
		
		
		if(TYPE=="COM"){
			#select daily values (aggregation-method)
			if(agg=="lm"){zen.d<-sub$zen.deg_lm;azi.d<-sub$azi.deg_lm}
			if(agg=="med"){zen.d<-sub$zen.deg_med;azi.d<-sub$azi.deg_med}
			if(agg=="med2"){zen.d<-sub$zen.deg_med2;azi.d<-sub$azi.deg_med2}
			
			#add uncertainty due to error in mast.o-measurements
			unc <- xs.interp.aspect(start.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), 
					end.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), length=10)
			time<- seq(from=range(data$generation_time)[1],
					to=range(data$generation_time)[2],length=10)
			lm.o<- lm(unc~time)
			unc.mast.o<- coef(lm.o)[1]+coef(lm.o)[2]*as.numeric(sub$generation_time)
			
			sigma.inc<- cbind(sub$zen.deg_sd^2,sub$cov_za.deg,sub$cov_za.deg,
					sub$azi.deg_sd^2)
			sigma.gps=cbind(sub$sdE.m^2, sub$cov_ne, sub$cov_ne,
					sub$cov_ne, sub$sdN.m^2,sub$cov_nh,
					sub$cov_he,sub$cov_nh,sub$sdh.m^2)
			zen<-rep(NA,len); azi<-zen; e.tmp<-zen; n.tmp<-zen
			
			for(i in 1:len){
				#get cov-matrix
				my.sigma.inc<-matrix(sigma.inc[i,], ncol=2)
				my.sigma.gps<-matrix(sigma.gps[i,], ncol=3)
				#test if covar-matrix is positive definite
				if(round(det(my.sigma.inc),digits=10)<=0 ){my.sigma.inc[,1][2]=my.sigma.inc[,2][1] =0}
				if(round(det(my.sigma.gps),digits=10)<=0 ){my.sigma.gps[,1][2:3]=my.sigma.gps[,2][c(1,3)]=
							my.sigma.gps[,3][1:2] =0}#if det < 0 set cov to 0
				
				#Incl
				uncert<- mvrnorm(n=1, mu=c(0,0), Sigma=my.sigma.inc)
				zen[i]<-zen.d[i] +uncert[1]
				azi[i]<-azi.d[i] +uncert[2]
				
				#gps
				uncert.gps<- mvrnorm(n=1, mu=c(0,0,0), Sigma=my.sigma.gps)
				e.tmp[i]<- sub$E.m[i]+uncert.gps[1]	
				n.tmp[i]<- sub$N.m[i]+uncert.gps[2]	
				h.tmp[i]<- sub$h.m[i]+uncert.gps[3]
			}# enf for len
			
			#add uncertainty due to mast.o-measurements to azi
			azi<- azi+unc.mast.o
			
			#get coordinates at mast-foot
			foot<- xs.com.pre(E.m=e.tmp,N.m=n.tmp,h.m=h.tmp,azi.deg=azi,
					zen.deg=zen,mast.h=sub$mast.h)
			
			e<- foot$x
			n<- foot$y
			h<- foot$z
		}# end COM
		
		#calculate linear regression and get coefficient and offset
		# E
		lme<- lm(e~sub$generation_time)
		v_e <- as.numeric(lme$coefficients[2])*86400
		a_e <- as.numeric(lme$coefficients[1])
		#N
		lmn<- lm(n~sub$generation_time)
		v_n <- as.numeric(lmn$coefficients[2])*86400
		a_n <- as.numeric(lmn$coefficients[1])
		#h
		lmh<- lm(h~sub$generation_time)
		v_h <- as.numeric(lmh$coefficients[2])*86400
		a_h <- as.numeric(lmh$coefficients[1])
		
		#calculate distance (horizontal velocity) and the azimuth of movement
		v_en <- sqrt(v_e^2+v_n^2+v_h^2);
		azi_v <- atan2(v_e,v_n)*180/pi
		if(azi_v<0) azi_v<- 360+azi_v
		
		#make vector
		now <- data.frame(a_e,v_e,a_n,v_n,a_h,v_h,v_en,azi_v)
		
		#make data-frame
		mc<- rbind(mc,now)
	}#end j
	
	
	#make nice data-frame
	names(mc)<-c("a_e","v_e","a_n","v_n","a_h","v_h","v_en","azi_en")
	
	return(mc)	
}#--  end 3D

############################################
#TODO:fix problem with neg covariance matrix and include binominal distribution
# include uncertainty of mast.o
###########################################
# sub, ind_now, sd.zen,sd.azi, cov.za,realizations=realizations
#-- INCL version ----------------------------------------------------------------
xs.incl.MC.lm<- function(data, ind_now, sd.zen,sd.azi,cov.za,realizations=1000, 
		epi=epi, pdf=NULL){
	#needs library MASS
	library(MASS)
	
	#select subset of data
	sub<- data[ind_now,]
	
	#get length of data-vector
	len= dim(sub)[1]
	
	#make MC simulations
	mc<- NULL
	for(j in 1:realizations){
		
		#add uncertainty due to error in mast.o-measurements
		unc <- xs.interp.aspect(start.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), 
				end.o=rnorm(n=1,mean=0, sd=data$sd.mast.o[1]), length=10)
		time<- seq(from=range(data$generation_time)[1],
				to=range(data$generation_time)[2],length=10)
		lm.o<- lm(unc~time)
		sub$mast.o<- data$mast.o[ind_now]+coef(lm.o)[1]+
				coef(lm.o)[2]*as.numeric(data$generation_time[ind_now])
		
		#use binomial distribution to estimate noise
#		zen<-rep(NA,len); azi<-zen
#		sigma=cbind(sd.zen^2, cov.za^2, cov.za^2, sd.azi^2)	
#		uncert<- mvrnorm(n=len, mu=c(0,0), Sigma=matrix(sigma, ncol=2))
#		for(i in 1:len){
#			zen[i]<- sub$zen.deg[i]+uncert[,1][i]
#			azi[i]<- sub$azi.deg[i]+uncert[,2][i]}
			zen<- sub$zen.deg+rnorm(n=len, mean=0, sd=sd.zen)	
			azi<- sub$azi.deg+rnorm(n=len, mean=0, sd=sd.azi)	
		
		
		#calculate linear regression and get coefficient and offset
		# Zenith
		lmZ<- lm(zen~sub$generation_time)
		v_zen <- as.numeric(lmZ$coefficients[2])
		a_zen <- as.numeric(lmZ$coefficients[1])
		# Azimuth
		lmA<- lm(azi~sub$generation_time)
		v_azi <- as.numeric(lmA$coefficients[2])
		a_azi <- as.numeric(lmA$coefficients[1])
		
		#make vector
		now <- data.frame(a_zen=a_zen,v_zen=v_zen,
				a_azi=a_azi,v_azi=v_azi)
		
		#make data-frame
		mc<- rbind(mc,now)
	}#end j
	

	return(mc)	
}# end inc

#==============================================================================
# PLOT TO TEST PERFORMANCE OF THE FUNCTTION "gps.MC.lm.2D"
#
# USES FUNCTION gps.MC.lm.2D
#
# INPUT gps.data with columns:
#	- generation_time
#	- position
#	- E.m
#	- N.m
#	- sdE.m
#	- sdN.m
#	- cov_xy
# in addition:
#	- realizations: number of MC-realizations
#	- window: time window used for regressio
#	- times: how many times the window (gives total data used)
#	- save.pdf: yes/no (save plots?)
#
# RETURNS: 
#	- lm.mc (data.frame) with columns: "a_e","v_e","a_n","v_n","v_en","azi_en"	
#	- PLOT 1: EAST-position and calculated regressions
#	- PLOT 2: azimuth of regressions
#
#
# to run function
#	graphics.off(); lm.mc<- gps.MC.lm.2D.test.plot(gps.data, realizations=10, window=6, times=12, save.pdf=0)
#
# Author: Vanessa
#==============================================================================
xs.gps.MC.lm.2D.test.plot<- function(gps.data, realizations=100, window=6, times=3, save.pdf=FALSE){
	
#select data for output
	max.out=(window*times)-times
	data.all<- gps.data[1:max.out,]
	pos<-data.all$position[1]
	
	#save regression plot
	if(save.pdf==1)pdf(plot.path&"plot_MClm_pos"&pos&"_"&max.out&".pdf")
	#plot original position data
	plot(y=data.all$E.m, x=data.all$generation_time, cex=0.5, ylab="position East", xlab="date", main="position "&pos)
	
	#Do MC-simulations and regression and plot regressions
	lm.mc<- NULL
	for(k in 1:realizations){
		lm.mc.now<- NULL
		
		for(i in seq(from=1,to=max.out-window,by=window)) { #dim(gps.data)[1])
			#select data within window
			data<-data.all[i:(i+window),]
			date<-data$generation_time
			#calculate regression with assigned uncertainty
			now<- xs.gps.MC.lm.2D(E.m=data$E.m, N.m=data$N.m, date=date, sd_e=data$sdE.m, 
					sd_n=data$sdN.m, cov_en=data$cov_en, realizations=1) #cov_ne is set to ZERO !!!!
			now$i<- as.factor(i)
			
			#add regression to plot
			col.i <- topo.colors(length(seq(from=1,to=max.out-window)) )
			lines(y=now$a_e+now$v_e*as.numeric(date), x=date, col=col.i[i])
			
			#save results of regression
			lm.mc.now<- rbind(lm.mc.now,now)
			
		}#end i
		lm.mc<-rbind(lm.mc, lm.mc.now)
	}# end k (MC simulations)
	
	
	#plot again points on top
	points(y=data.all$E.m, x=data.all$generation_time, pch=16, cex=0.4, col="red")
	if(save.pdf==1)dev.off()
	
	if(save.pdf==1)pdf(plot.path&"plot_MClm_azi_pos"&pos&"_"&max.out&".pdf")
	if(save.pdf==0)dev.new() 
	plot(lm.mc$azi_en, ylab="Azimuth of velocity [deg from North]",col=lm.mc$i,
			main="Position "&pos, xlab="simulations")
	if(save.pdf==1)dev.off()
	
	return(lm.mc)
}


#==============================================================================
# CALCULATE SIGNAL TO NOISE RATIO
#
# INPUT data:
#	- v: coefficients (lengths equals number of MC-simulations (realizations)
#	- metric
#
# RETURNS: 
#	- snr	
#	
# Author: Vanessa
#==============================================================================
xs.gps.snr<-function(v,time, metric=1){
	signal<- abs(mean(v))
	snr <- (signal/sd(v))*metric	#how to handle negative values???
#	if(signal==0& sd(v)==0) snr=0
#	if(signal!=0& sd(v)==0) snr=signal
	return(snr)
}


#===============================================================================
# CALCULATE VELOCITY
#
# Removes outliers for calculation. Data point is an outlier if difference between 
# fitted and measured value is higher than 5 times the standard deviation.
# Uses Monte-Carlo to propagate uncertainty. Calculates the rate of change based
# on linear regressions. The number of used data-points depends on the signal-to-noise  
# ratio. If the SNR-condition is notfulfilled, the time lag is increased until 
# it is met.Once it is met it is tested if a smaller time lag can be used, 
# if a backward loop is used.
#
#
# TODO:
#	- Test the function
#
# INPUT data:
#	- v: coefficients (lengths equals number of MC-simulations (realizations)
#	- metric
#
#
#	- agg="lm"/"med": aggregation of inc-data
#
# RETURNS: 
#	- v_en: velocity (m/s)	
#	- v_en.sd_ standar deviation of vel
#	- azi_en: direction (azimuth as deg eastwards from North) of vel
#	- time_beg: start-time of period with this vel
#	- time_end: end-time of period with this vel
#	- position
#	- device_id
#	- fwd: ("fwd"=forward loop,"bwd"=backward loop or 
#				"none"= (indicates, that it is not significant. Is used for points not included 
#						in backward loop. But thei are not NA) 
#	
#
# Author: Vanessa Wirz, Stephan Gruber
#============================================================================ ====================
xs.gps.rate.lm.1D<-function(data, component, dev.change, snr=5,realizations=100,
		rm.outliers=FALSE, pdf.file=NULL) {
	#ensure only one position is processed
	if (length(unique(data$position))>1) return(NA)
	
	
	
	#select columns and make sure data exists
	if(is.element(component, c("E.m","N.m","h.m","inc.azi","inc.zen"))==0)return(NULL)
	if(component=="E.m"){
		data<- data[is.na(data$E.m)==0 , ]
	my.data<- data[, c("generation_time","E.m","sdE.m")] }
	if(component=="N.m"){
		data<- data[is.na(data$N.m)==0 , ]
		my.data<- data[, c("generation_time","N.m","sdN.m")]}
	if(component=="h.m"){
		data<- data[is.na(data$h.m)==0 , ]
		my.data<- data[, c("generation_time","h.m","sdh.m")]}
	
	#remove outliers
	if(rm.outliers==TRUE){
	ind.outl<- xs.remove.outlier(x=my.data[,2],sd.x=my.data[,3],
			date=my.data[,1], name=component)
	my.data<- my.data[ind.outl==0,]}
	
	#set general names
	names(my.data)<- c("generation_time","X","sdX")

	#plot mc-realizations
	if(is.null(pdf.file)==0){
		CTI<-0.393700787 #cm to inch by multiplication
		width <-20*CTI
		height<-10*CTI
		pdf(pdf.file,width,height)
		plot(my.data$X,x=my.data$generation_time,type="l");
	}
	
	#loop over device episodes
	rate<-NULL; rate.NA<- NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(my.data,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$generation_time)
		days<-as.numeric(sub$generation_time)/86400
		
		#in case no data is available for this period
		if(dim(sub)[1]<2){
				now<-data.frame(time_beg=dev.change$dbeg[epi],
						time_end=dev.change$dend[epi],
						position= dev.change$position[epi], 
						device_id=dev.change$device_id[epi],
						v= NA, v.sd=NA,
						v.q1= NA,v.q2= NA,
						a=NA, fwd=NA) 
		}else{
			#insert NA where orignial data have gaps
			time_beg.NA<-sub$generation_time[c(diff(sub$generation_time)>2, FALSE)]
			time_end.NA<- sub$generation_time[c(FALSE,diff(sub$generation_time)>2)]
			
			now.NA<-data.frame(time_beg=time_beg.NA,
					time_end=time_end.NA,
					position= rep(dev.change$position[epi], length(time_beg.NA)), 
					device_id=rep(dev.change$device_id[epi], length(time_beg.NA)),
					v= rep(NA,length(time_beg.NA)), v.sd=rep(NA,length(time_beg.NA)),
					v.q1= rep(NA,length(time_beg.NA)),v.q2= rep(NA,length(time_beg.NA)),
					a=rep(NA,length(time_beg.NA)), fwd=rep(NA,length(time_beg.NA))) 
			rate.NA<- rbind(rate.NA, now.NA)
			
		##START WITH LOOP	
		f1<-1 #start with first value
		while (f1 <= (len-1)) { #FORWARD begin time 
			
			for (f2 in (f1+1):len) { #FORWARD end time
				
				#model fit: MC simulations and regressions to calc vel
				sub.now<-sub[f1:f2,]
				mc.now<- xs.gps.MC.lm.1D(date=sub.now$generation_time, E.m=sub.now$X, 
						sd_e=sub.now$sdX, realizations=realizations)
				
				#add time to data frame
				ndays<-days[f2]-days[f1]
				mc.now$time <-rep(ndays,realizations)
				
				#FORWARD SNR: quality criterion
				#calculate SNR
				snr.now<- xs.gps.snr(v=mc.now$v, metric=1)
				if(is.na(snr.now)==1)snr.now=0 #if snr.now=NA set to zero
				
				if (snr.now >= snr) {
					back.better<-FALSE #is a better solution backwards possible?
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting bakwards, starting with b1
						
						for (b2 in (b1-1):(f1+1)) { #backwards start time
							sub.now.b<-sub[b2:b1,]
							mc.now.b<- xs.gps.MC.lm.1D(date=sub.now.b$generation_time, E.m=sub.now.b$X, 
									sd_e=sub.now.b$sdX, realizations=realizations)
							
							#add time to data frame
							ndays.b<-days[b1]-days[b2]
							mc.now.b$time<- rep(ndays.b,realizations)
							
							#BACKWARD SNR: quality criterion
							snr.now.b<- xs.gps.snr(v=mc.now.b$v, metric=1)
							
							if  (snr.now.b >= snr) {
								back.better<-TRUE
								
								#add regression for remaining values
								if(b2 <= f1) mc.tmp<-NULL
								if(b2 > f1){
									sub.tmp<- sub[f1:b2,]
									mc.tmp<- xs.gps.MC.lm.1D(date=sub.tmp$generation_time, E.m=sub.tmp$X, 
											sd_e=sub.tmp$sdX, realizations=realizations)
									
									#add time to data frame
									ndays.tmp<-days[b2]-days[f1]
									mc.tmp$time<- rep(ndays.tmp,realizations)
								}
								
								break #break backward width loop
							}#end if backward quality
						}#end b2 for-loop 	
					}#=====END BACKWARD LOOP=================================
					#assign backward loop result
					if (back.better == TRUE) { #use backward
						now<-data.frame(time_beg=sub$generation_time[b2],
								time_end=sub$generation_time[b1],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								v= mean(mc.now.b$v), 
								v.sd=sd(mc.now.b$v),
								v.q1=quantile(mc.now.b$v, prob=0.05),
								v.q2=quantile(mc.now.b$v, prob=0.95),
								a=mean(mc.now.b$a),fwd="bwd")	
						#plot mc-regressions
						if(is.null(pdf.file)==0){
								for(i in 1:realizations){
									lines(y=mc.now.b$a[i] +mc.now.b$v[i]*as.numeric(sub.now.b$generation_time)/86400, 
											x=sub.now.b$generation_time, col=f1)}	
								lines(y=mean(mc.now.b$a) + mean(mc.now.b$v)*as.numeric(sub.now.b$generation_time)/86400, 
										x=sub.now.b$generation_time, col="red")
							}
						
						#print("BACKWARD")
						
						#add values from remaining (not significant) data points
						if(is.null(mc.tmp)==0){
							now.tmp<-data.frame(time_beg=sub$generation_time[f1],
									time_end=sub$generation_time[b2],
									position= dev.change$position[epi], 
									device_id=dev.change$device_id[epi],
									v= mean(mc.tmp$v), 
									v.sd=sd(mc.tmp$v),
									v.q1=quantile(mc.tmp$v, prob=0.05),
									v.q2=quantile(mc.tmp$v, prob=0.95),
									a=mean(mc.tmp$a),fwd="none")	
							now<- rbind(now,now.tmp)	
							#plot mc-regressions
							if(is.null(pdf.file)==0){
								for(i in 1:realizations){
									lines(y=mc.tmp$a[i] +mc.tmp$v[i]*as.numeric(sub.tmp$generation_time)/86400, 
											x=sub.tmp$generation_time, col=f1)}	
								lines(y=mean(mc.tmp$a) + mean(mc.tmp$v)*as.numeric(sub.tmp$generation_time)/86400, 
										x=sub.tmp$generation_time, col="red")
							}
						}
					}#end assign back ward
					
					#assign forward loop results	
					if (back.better == FALSE) { #use forward
						now<-data.frame(time_beg=sub$generation_time[f1],
								time_end=sub$generation_time[f2],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								v= mean(mc.now$v), 
								v.sd=sd(mc.now$v),
								v.q1=quantile(mc.now$v, prob=0.05),
								v.q2=quantile(mc.now$v, prob=0.95),
								a=mean(mc.now$a), fwd="fwd") 
						#plot mc-regressions
						if(is.null(pdf.file)==0){
							for(i in 1:realizations){
							lines(y=mc.now$a[i] + mc.now$v[i]*as.numeric(sub.now$generation_time)/86400, 
									x=sub.now$generation_time, col=f1)}	
							lines(y=mean(mc.now$a) + mean(mc.now$v)*as.numeric(sub.now$generation_time)/86400, 
								x=sub.now$generation_time, col="red")
						}
						#print("FORWARD")
					}#end assign forward
					
					rate<-rbind(rate,now)
					break #break t2 loop
				}#if SNR forward OK
				
			}#f2 for-loop
			f1<-f2
		}#f1 while loop
	  }#condition that for this episod there are enough data-points

	  }#episodes for-loop
	 
	  #close plot
	  if(is.null(pdf.file)==0)dev.off()
	  
	#add NA and order by generation_time  
	rate<-rbind(rate,rate.NA)
	rate<- rate[order(rate$time_beg),]
	
	print("finished")
	
	#return result
	return(rate)
} #----------------------------------------------------------------------------

#---MC LM calc rate in 2D/3D---------------------------------------------------
xs.gps.rate.lm<-function(data, dev.change, snr=snr,realizations=realizations, TYPE="GPS",
		rm.outliers=FALSE, DIM="2D",agg=NULL) {
	#make sure data exists
	if (is.null(data)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	
	#exclude data with NA-values (removed outliers)
	data<- data[is.na(data$E.m)==0 & is.na(data$N.m)==0 & is.na(data$h.m)==0 , ]
	
	
	#ensure only one position is processed
	if (length(unique(data$position))>1) return(NA)
	#remove outliers
	if(rm.outliers==TRUE){
	if(TYPE=="COM" | TYPE=="GPS"){
		ind.outl.N<- xs.remove.outlier(x=data$N.m,sd.x=data$sdN.m, #in North
			date=data$generation_time, name="N.m")
		ind.outl.E<- xs.remove.outlier(x=data$E.m,sd.x=data$sdE.m,#in East
			date=data$generation_time, name="E.m")
		data<- data[ind.outl.N==0 & ind.outl.E==0,]}
	if(TYPE=="COM"){
		ind.outl.X <- xs.remove.outlier(x=data[,"zen.deg_"&agg],
				sd.x=data[,"zen.deg_sd"&substr(agg,4,4)], #X
				date=data$generation_time, name="zen")
		ind.outl.Y <- xs.remove.outlier(x=data[,"azi.deg_"&agg],
				sd.x=data[,"azi.deg_sd"&substr(agg,4,4)], #Y
				date=data$generation_time, name="azi")
		data<- data[ind.outl.X==0 & ind.outl.Y==0,]	}
	}
	
	if(TYPE=="COM"){names(data)[2]<-"position" #change name "position.x" to "position"
	data<- data[is.na(data[,"zen.deg_"&agg])== 0 & is.na(data[,"azi.deg_"&agg])== 0,]}
	
	#loop over device episodes
	rate<-NULL; rate.NA<- NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(data,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)
		days<-as.numeric(sub$generation_time)/86400
		
		#in case no data is available for this period, insert NA
		if(dim(sub)[1]<2){
			now<-data.frame(time_beg=dev.change$dbeg[epi],
					time_end=dev.change$dend[epi],
					position= dev.change$position[epi], 
					device_id=dev.change$device_id[epi],
					v_en= NA, #change do m/day
					v_en.sd=NA,#change do m/day
					v_en.q1=NA,#quantile 0.05
					v_en.q2=NA,#quantile 0.95
					azi_en=NA, fwd=NA) 	}
		#if enough data exists, go forward	
		if(dim(sub)[1]>2){
			#insert NA where orignial data have gaps
			time_beg.NA<-sub$generation_time[c(diff(sub$generation_time)>2, FALSE)]
			time_end.NA<- sub$generation_time[c(FALSE,diff(sub$generation_time)>2)]
			
			now.NA<-data.frame(time_beg=time_beg.NA, time_end=time_end.NA,
					position= rep(dev.change$position[epi], length(time_beg.NA)), 
					device_id=rep(dev.change$device_id[epi], length(time_beg.NA)),
					v_en= rep(NA,length(time_beg.NA)), 
					v_en.sd=rep(NA,length(time_beg.NA)),
					v_en.q1=rep(NA,length(time_beg.NA)),
					v_en.q2=rep(NA,length(time_beg.NA)),
					azi_en=rep(NA,length(time_beg.NA)), fwd=rep(NA,length(time_beg.NA))) 	
			rate.NA<- rbind(rate.NA, now.NA)
			
			
	##START WITH LOOP	

		f1<-1 #start with first value
		while (f1 <= (len-1)) { #FORWARD begin time 
			
			for (f2 in (f1+1):len) { #FORWARD end time
				
				#model fit: MC simulations and regressions to calc vel
				ind_now<-f1:f2
				if(DIM=="2D"){mc.now<- xs.gps.MC.lm.2D(data=sub, ind_now, 
							realizations=realizations, TYPE=TYPE,agg=agg)}
				if(DIM=="3D"){mc.now<- xs.gps.MC.lm.3D(data=sub, ind_now, 
							realizations=realizations, TYPE=TYPE,agg=agg)}
				
				
				#add time to data frame
				ndays<-days[f2]-days[f1]
				mc.now$time<-rep(ndays,realizations)
				
				#FORWARD SNR: quality criterion
				#calculate SNR
				snr.now<- xs.gps.snr(v=mc.now$v_en, metric=1)
				if(is.na(snr.now)==1)snr.now=0
				
				if (snr.now >= snr) {
					back.better<-FALSE #is a better solution backwards possible?
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting bakwards, starting with b1
				
						for (b2 in (b1-1):(f1+1)) {
							ind_now.b<-b2:b1
							if(DIM=="2D"){mc.now.b<- xs.gps.MC.lm.2D(sub, ind_now.b, 
										realizations=realizations, TYPE=TYPE,agg=agg)}
							if(DIM=="3D"){mc.now.b<- xs.gps.MC.lm.3D(sub, ind_now.b, 
										realizations=realizations, TYPE=TYPE,agg=agg)}
				
							#add time to data frame
							ndays.b<-days[b1]-days[b2]
							mc.now.b$time<- rep(ndays.b,realizations)
				
						#BACKWARD SNR: quality criterion
						snr.now.b<- xs.gps.snr(v=mc.now.b$v_en, metric=1)
						
						if  (snr.now.b >= snr) {
							back.better<-TRUE
							
							#add regression for remaining values
							if(b2 <= f1) mc.tmp<-NULL
							if(b2 > f1){
								ind_tmp<- f1:b2
								if(DIM=="2D"){mc.tmp<- xs.gps.MC.lm.2D(sub,ind_tmp, 
											realizations=realizations, TYPE=TYPE,agg=agg)}
								if(DIM=="3D"){mc.tmp<- xs.gps.MC.lm.3D(sub,ind_tmp, 
											realizations=realizations, TYPE=TYPE,agg=agg)}
								
								#add time to data frame
								ndays.tmp<-days[b2]-days[f1]
								mc.tmp$time<- rep(ndays.tmp,realizations)
							}
							
							break #break backward width loop
						}#end if backward quality
					}#end b2 for-loop 	
				}#=====END BACKWARD LOOP=================================
					#assign backward loop result
					if (back.better == TRUE) { #use backward
						now.b<-data.frame(time_beg=sub$generation_time[b2],
								time_end=sub$generation_time[b1],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								v_en= mean(mc.now.b$v_en), #change do m/day
								v_en.sd=sd(mc.now.b$v_en),#change do m/day
								v_en.q1=quantile(mc.now.b$v_en, prob=0.05),
								v_en.q2=quantile(mc.now.b$v_en, prob=0.95),
								azi_en=mean(mc.now.b$azi_en), fwd="bwd")	
			
						
						
						#add values from remaining (not significant) data points
						if(is.null(mc.tmp)==0){
							now.tmp<-data.frame(time_beg=sub$generation_time[f1],
									time_end=sub$generation_time[b2],
									position= dev.change$position[epi], 
									device_id=dev.change$device_id[epi],
									v_en= mean(mc.tmp$v_en), #change do m/day
									v_en.sd=sd(mc.tmp$v_en),#change do m/day
									v_en.q1=quantile(mc.tmp$v_en, prob=0.05),
									v_en.q2=quantile(mc.tmp$v_en, prob=0.95),
									azi_en=mean(mc.tmp$azi_en), fwd="none")	
						now<- rbind(now.b,now.tmp)	
						}
					}
					#assign forward loop results	
					if (back.better == FALSE) { #use forward
						now<-data.frame(time_beg=sub$generation_time[f1],
								time_end=sub$generation_time[f2],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								v_en= mean(mc.now$v_en), #change do m/day
								v_en.sd=sd(mc.now$v_en),#change do m/day
								v_en.q1=quantile(mc.now$v_en, prob=0.05),
								v_en.q2=quantile(mc.now$v_en, prob=0.95),
								azi_en=mean(mc.now$azi_en), fwd="fwd") 	
						
					}
					
					rate<-rbind(rate,now);
					break #break t2 loop
				}#if SNR forward OK
				
			}#f2 for-loop
			f1<-f2
		}#f1 while loop
		}#condition that for this subset (epi) there are enough data-points	
	}#end episodes for-loop
	rate<-rbind(rate,rate.NA)
	#order data and return result
	rate<- rate[order(rate$time_beg),]
	
	print("finished")
			
	return(rate)
} #----------------------------------------------------------------------------

#---MC LM calc inc rate -------------------------------------------------------
# component: azi, zen
xs.inc.rate.lm<-function(data, dev.change, snr=5,realizations=100,rm.outliers=FALSE) {
	
	#remove data-gaps
	data<- data[is.na(data$zen.deg)==0 & is.na(data$azi.deg)==0, ]
	
	#ensure only one position is processed
	if (length(unique(data$position))>1) return(NA)

	#loop over device episodes
	rate<-NULL; rate.NA<- NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(data,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$generation_time)
		days<-as.numeric(sub$generation_time)/86400
		
		#in case no data is available for this period
		if(dim(sub)[1]<2){
			now<-rep(NA,8)
		}else{
			
			#insert NA where orignial data have gaps (>1 day)
			time_beg.NA<-sub$generation_time[c(diff(sub$generation_time)>86400, FALSE)]
			time_end.NA<- sub$generation_time[c(FALSE,diff(sub$generation_time)>86400)]
			
			now.NA<-data.frame(time_beg=time_beg.NA,
					time_end=time_end.NA,
					position= rep(dev.change$position[epi], length(time_beg.NA)), 
					device_id=rep(dev.change$device_id[epi], length(time_beg.NA)),
					v_zen= rep(NA,length(time_beg.NA)), v_zen.sd=rep(NA,length(time_beg.NA)),
					v_zen.q1=rep(NA,length(time_beg.NA)),v_zen.q2=rep(NA,length(time_beg.NA)),
					v_azi= rep(NA,length(time_beg.NA)), v_azi.sd=rep(NA,length(time_beg.NA)),
					v_azi.q1=rep(NA,length(time_beg.NA)),v_azi.q2=rep(NA,length(time_beg.NA)),
					fwd=rep(NA,length(time_beg.NA))) 	
			rate.NA<- rbind(rate.NA, now.NA)	
			
			##START WITH LOOP	
			f1<-1 #start with first value
			while (f1 <= (len-12)) { #FORWARD begin time 
				
				for (f2 in seq(from=f1+12, to=len, by=12)) { #FORWARD end time (start with values of 1h
					#get data for period
					ind_now<- f1:f2
					
					#calculate mean and covariance
					sd.zen<- sd(sub$zen.deg[ind_now], na.rm=T)
					sd.azi<- sd(sub$azi.deg[ind_now],na.rm=T)
					cov.za<- cov(sub$zen.deg[ind_now],sub$azi.deg[ind_now])
					
					#model fit: MC simulations and regressions to calc vel
					mc.now<- xs.incl.MC.lm(sub, ind_now, sd.zen,sd.azi, cov.za,realizations=realizations)
					
					#add time to data frame
					ndays<-days[f2]-days[f1]
					mc.now$time <-rep(ndays,realizations)
					
					#FORWARD SNR: quality criterion
					#calculate SNR
					snr.now<- max(xs.gps.snr(v=mc.now$v_zen, metric=1),
							xs.gps.snr(v=mc.now$v_azi, metric=1))
					
					if (snr.now >= snr) {
						back.better<-FALSE #is a better solution backwards possible?
						back.len<-1+f2-f1  #total amount of time to work with
						#=====BACKWARD LOOP========================================
						if (back.len>=14) { #only usful for lags larger than start-lag
							b1<-f2 #counting bakwards, starting with b1
							
							for (b2 in seq(from=(b1-12),to=(f1+1),by=-12) ) { #backwards start time
								ind_now.b<-b2:b1
							
								#calculate mean and covariance
								sd.zen.b<- sd(sub$zen.deg[ind_now.b], na.rm=T)
								sd.azi.b<- sd(sub$azi.deg[ind_now.b],na.rm=T)
								cov.za.b<- cov(sub$zen.deg[ind_now.b],sub$azi.deg[ind_now.b])
								
								#model fit: MC simulations and regressions to calc vel
								mc.now.b<- xs.incl.MC.lm(sub,  ind_now=ind_now.b, sd.zen.b,sd.azi.b, 
										cov.za.b,realizations=realizations)
								
								#add time to data frame
								ndays.b<-days[b1]-days[b2]
								mc.now.b$time<- rep(ndays.b,realizations)
								
								#BACKWARD SNR: quality criterion
								snr.now.b<- max(xs.gps.snr(v=mc.now.b$v_zen, metric=1),
										xs.gps.snr(v=mc.now.b$v_azi, metric=1))
								
								if  (snr.now.b >= snr) {
									back.better<-TRUE
									
									#add regression for remaining values
									if(b2 <= f1) mc.tmp<-NULL
									if(b2 > f1){
										ind_tmp<- f1:b2
								
										#calculate mean and covariance
										sd.zen.t<- sd(sub$zen.deg[ind_tmp], na.rm=T)
										sd.azi.t<- sd(sub$azi.deg[ind_tmp],na.rm=T)
										cov.za.t<- cov(sub$zen.deg[ind_tmp],sub$azi.deg[ind_tmp])
										
										#model fit: MC simulations and regressions to calc vel
										mc.tmp<- xs.incl.MC.lm(sub,  ind_now=ind_tmp, sd.zen.t,sd.azi.t, 
												cov.za.t,realizations=realizations)
										
										#add time to data frame
										ndays.tmp<-days[b2]-days[f1]
										mc.tmp$time<- rep(ndays.tmp,realizations)
									}
									
									break #break backward width loop
								}#end if backward quality
							}#end b2 for-loop 	
						}#=====END BACKWARD LOOP=================================
						#assign backward loop result
						if (back.better == TRUE) { #use backward
							now<-data.frame(time_beg=sub$generation_time[b2],
									time_end=sub$generation_time[b1],
									position= dev.change$position[epi], 
									device_id=dev.change$device_id[epi],
									v_zen= mean(mc.now.b$v_zen), 
									v_zen.sd=sd(mc.now.b$v_zen),
									v_zen.q1=quantile(mc.now.b$v_zen, prob=0.05),
									v_zen.q2=quantile(mc.now.b$v_zen, prob=0.95),
									a_zen=mean(mc.now.b$a_zen),
									v_azi= mean(mc.now.b$v_azi), 
									v_azi.sd=sd(mc.now.b$v_azi),
									v_azi.q1=quantile(mc.now.b$v_azi, prob=0.05),
									v_azi.q2=quantile(mc.now.b$v_azi, prob=0.95),
									a_azi=mean(mc.now.b$a_azi),
									fwd="bwd")	
							
							#add values from remaining (not significant) data points
							if(is.null(mc.tmp)==0){
								now.tmp<-data.frame(time_beg=sub$generation_time[f1],
										time_end=sub$generation_time[b2],
										position= dev.change$position[epi], 
										device_id=dev.change$device_id[epi],
										v_zen= mean(mc.tmp$v_zen), 
										v_zen.sd=sd(mc.tmp$v_zen),
										v_zen.q1=quantile(mc.tmp$v_zen, prob=0.05),
										v_zen.q2=quantile(mc.tmp$v_zen, prob=0.95),
										a_zen=mean(mc.tmp$a_zen),
										v_azi= mean(mc.tmp$v_azi), 
										v_azi.sd=sd(mc.tmp$v_azi),
										v_azi.q1=quantile(mc.tmp$v_azi, prob=0.05),
										v_azi.q2=quantile(mc.tmp$v_azi, prob=0.95),
										a_azi=mean(mc.tmp$a_azi),
										fwd="bwd")	
								now<- rbind(now,now.tmp)	
							}
						}
						#assign forward loop results	
						if (back.better == FALSE) { #use forward
							now<-data.frame(time_beg=sub$generation_time[f1],
									time_end=sub$generation_time[f2],
									position= dev.change$position[epi], 
									device_id=dev.change$device_id[epi],
									v_zen= mean(mc.now$v_zen), 
									v_zen.sd=sd(mc.now$v_zen),
									v_zen.q1=quantile(mc.now$v_zen, prob=0.05),
									v_zen.q2=quantile(mc.now$v_zen, prob=0.95),
									a_zen=mean(mc.now$a_zen),
									v_azi= mean(mc.now$v_azi), 
									v_azi.sd=sd(mc.now$v_azi),
									v_azi.q1=quantile(mc.now$v_azi, prob=0.05),
									v_azi.q2=quantile(mc.now$v_azi, prob=0.95),
									a_azi=mean(mc.now$a_azi),
									fwd="bwd")
							
						}
						
						rate<-rbind(rate,now)
						break #break t2 loop
					}#if SNR forward OK
					
				}#f2 for-loop
				f1<-f2
			}#f1 while loop
		}#condition that for this episod there are enough data-points
		
	}#episodes for-loop
	
	rate<-rbind(rate,rate.NA)
	rate<- rate[order(rate$time_beg),]

	#return result
	return(rate)
} #----------------------------------------------------------------------------



#==============================================================================
# GET DAILY RATE-VALUES (returns no data where originally was no data (NA) )
#
# INPUT:
#	-rate (data frame) with columns:
#
# RETURNS: rate.daily (data.frame -> one value per day); with columns:
#	- date     : POSIXct
#	- position : num  
#	- device_id: Factor 
#	- rate     : num  
#	- rate.sd  : num  
#	- rate.azi : num  
#	- fwd      : logi  
#
#==============================================================================
xs.rate.daily <- function(rate, TYPE="GPS1D"){
	rate.daily<- NULL
	
	#For 1D
	if(TYPE=="GPS1D"){
	for(r in 1:nrow(rate) ){#loop throug rows and make daily values	
	date<- seq(from=rate$time_beg[r], to=rate$time_end[r], by="day")[-1]
	if(is.na(rate$v[r])==1)date<- c(rate$time_beg[r],date)
	len=length(date)
		now<- data.frame(date=date, position=rep(rate$position[r], len),
				device_id=rep(rate$device_id[r], len),
				rate=rep(rate$v[r], len),
				rate.sd=rep(rate$v.sd[r], len),
				rate.q1=rep(rate$v.q1[r], len),
				rate.q2=rep(rate$v.q2[r], len),
				a=rep(rate$a[r], len),
				fwd=rep(rate$fwd[r], len) )
		rate.daily<- rbind(rate.daily,now)
	}
	}#end 1D
	
	#For 2D
	if(TYPE=="GPS2D"){
		for(r in 1:nrow(rate) ){#loop throug rows and make daily values
			date<- seq(from=rate$time_beg[r], to=rate$time_end[r], by="day")[-1]
			if(is.na(rate$v_en[r])==1)date<- c(rate$time_beg[r],date)
			len=length(date)
			now<- data.frame(date=date, position=rep(rate$position[r], len),
					device_id=rep(rate$device_id[r], len),
					rate=rep(rate$v_en[r], len),
					rate.sd=rep(rate$v_en.sd[r], len),
					#rate.q1=rep(rate$v_en.q1[r], len),
					#rate.q2=rep(rate$v_en.q2[r], len),
					rate.azi=rep(rate$azi_en[r], len),
					fwd=rep(rate$fwd[r], len) )
			rate.daily<- rbind(rate.daily,now)
		}
	}#end 2D		
	
	if(TYPE=="INC"){
		for(r in 1:nrow(rate) ){#loop throug rows and make daily values
			date<- seq(from=rate$time_beg[r], to=rate$time_end[r], by="5 mins")[-1]
			if(is.na(rate$v_zen[r])==1)date<- c(rate$time_beg[r],date)
			len=length(date)
			now<- data.frame(date=date, position=rep(rate$position[r], len),
					device_id=rep(rate$device_id[r], len),
					rate=rep(rate$v_zen[r], len),
					rate.sd=rep(rate$v_zen.sd[r], len),
					rate_azi=rep(rate$v_azi[r], len),
					rate_azi.sd=rep(rate$v_zen.sd[r], len),
					fwd=rep(rate$fwd[r], len) )
			rate.daily<- rbind(rate.daily,now)
		}
	}
	#order by date and then by rate
	rate.daily<- rate.daily[with(rate.daily,order(date, rate)), ]
	
	#remove dublicates
	rate.daily<-rate.daily[duplicated(rate.daily$date, fromLast=TRUE)==0,]
	
	#remove NA
		#rate.daily<- rate.daily[is.na(rate.daily$rate)==0,]
	
	#return
	return(rate.daily)
}


#==============================================================================
# PLOT TO TEST PERFORMANCE OF THE FUNCTTION "xs.gps.rate.lm.1D"
#
# USES FUNCTION xs.gps.rate.lm.1D, gps.MC.lm.1D
#
# INPUT gps.data with columns:
#	- generation_time
#	- position
#	- E.m
#	- sdE.m
# in addition:
#	- realizations: number of MC-realizations
#	- save.pdf: yes/no (save plots?)
#
# RETURNS: 
#	- rate (data.frame) with columns: 	
#	- PLOT 1: EAST-position and calculated regressions
#
#
# to run function
#	xs.plot.gps.MC.lm.1D.test(gps.data, component="E.m",dev.change,
#				snr=15,realizations=100, save.pdf=TRUE) 
#			
#
# Author: Vanessa
#==============================================================================
xs.plot.gps.MC.lm.1D.test<- function(gps.data, component="E.m", dev.change, 
		snr=15, realizations=100, save.pdf=FALSE){

	#make x axis (date)
	datelabel<- seq(from=min(gps.data$generation_time), 
			to=max(gps.data$generation_time), by="1 month")
	
	#save plot
	if(save.pdf==1)pdf(plot.path&"plot_rate1D_pos"&pos&".pdf")
	
	#plot GPS-data (daily solutions)	
	plot(gps.data[,component], x=gps.data$generation_time, ylab=component, xlab="", 
		main="Position "&gps.data$position[1], pch=16, axes=F)
	
	#make axis	
	axis(2, las=3) 
	axis(1, at=datelabel, tick= T, labels= strftime(datelabel,"%Y/%m"));box()

	#calculate linear regressions of gps 
	rate<- xs.gps.rate.lm.1D(gps.data, component, dev.change, snr,realizations)
	
	#add regressions to plot
	col.i <- rep(rainbow(4),length.out=dim(rate)[1])
	for(i in 1:dim(rate)[1]){
		date=seq(from=rate$time_beg[i], to=rate$time_end[i], by="day")
	lines(y=rate$a[i]+rate$v[i]*as.numeric(as.Date(date)), x=date, lwd=2, col=col.i[i])
	
	}#end for-loop

	#add arrows
	ymax<-max(gps.data[,component])+1
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,ymax)
	
	if(save.pdf==1){dev.off()}
	
	return(rate)
}



#==============================================================================
# CALCULATE SIMPEL VELOCITY (DISTANCE/TIME)
#
#calculates simple velocity. Distance/time. if time-difference between two 
# data-points is larger than 1 day, NA is inserted. Is done seperatly for 
# each device-id.
#
# INPUT gps.data with columns:
#	- generation_time
#	- position
#	- E.m
#	- N.m
#	- h.m
#	- sdE.m/sdN.m/sdh.m
#
# RETURNS: 
#	- rate (data.frame) with columns: 	
#
#
# Author: Vanessa
#==============================================================================

#plot -------------------------------------------------------------------------
xs.gps.rate.simple.plot<- function(rate.simple,dev.change){
	pos=unique(dev.change$position)
library(ggplot2);library(reshape)
dat <- data.frame(x=rate.simple$generation_time, y1=rate.simple$velHor, y2=rate.simple$velHor.f,
		y3=rate.simple$vel3D, y4=rate.simple$vel3D.f)
dat.m <- melt.data.frame(rate.simple[,c(1,4:7)], "generation_time")

#plot RATE
#pdf(file=plot.path&"pos-"&pos&"_"&agg&by&"simple_velocity.pdf",sep=""))	
ylim1=c(0, max(dat$y1,dat$y2,na.rm=T));ylim2=c(0, max(dat$y3,dat$y4,na.rm=T))
ggplot(dat.m, aes(x=generation_time, value, colour = variable))+  
		geom_line() + facet_wrap(~ variable, ncol = 1, scales = "free_y") +
		theme(legend.position="none") + ggtitle("Simple velocities for position "&pos&" with med over "&by&" days")+
		xlab(" ") +ylab("velocity [m/d]")+ geom_vline(xintercept=as.numeric(dev.change$dbeg),
				linetype=2, colour="darkgrey")+ geom_vline(xintercept=as.numeric(dev.change$dend),
				linetype=2, colour="darkgrey")
#dev.off()
}
#REAL FUNCTION-----------------------------------------------------------------
xs.gps.rate.simple<-function(gps.data, inc.daily, dev.change,agg="med", by=by){
		
	#merge data-frames
	gps.inc<- merge(gps.data, inc.daily, by = "generation_time",sort = TRUE,incomparables = NA)
	names(gps.inc)[2]<- "position"
	#POSITIVE FINIT COVAR-MATRIX!!!
	gps.inc$cov_za.deg<-rep(0,dim(gps.inc)[1])
	
	rate.s<-NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(gps.inc,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$generation_time)
		days<-as.numeric(sub$generation_time)/86400
		
		#ANTENNA
		#distance between two data-points
		dE <- diff(sub$E.m,lag=1)
		dN <- diff(sub$N.m,lag=1)
		dh <- diff(sub$h.m,lag=1)
		dHor<- sqrt(dE^2+dN^2)
		d3D<- sqrt(dE^2+dN^2+dh^2)
		velHor_azi <- atan2(dE,dN)*180/pi
		dTime<- as.numeric(diff(sub$generation_time,lag=1))
		#insert NA for time-difference > 1 Day
		dTime[dTime>1]<- NA
		
		#FOOT
		foot<- xs.com.pre(sub$E.m,sub$N.m,sub$h.m,
				sub[,"azi.deg_"&agg],sub[,"zen.deg_"&agg],sub$mast.h)
		dE.f <- diff(foot$x,lag=1)
		dN.f <- diff(foot$y,lag=1)
		dh.f <- diff(foot$z,lag=1)
		dHor.f<- sqrt(dE.f^2+dN.f^2)
		velHor_azi.f<-atan2(dE.f,dN.f)*180/pi;
		d3D.f<- sqrt(dE.f^2+dN.f^2+dh.f^2)
		
		#standard deviation
		#see wikipedia:http://de.wikipedia.org/wiki/Fehlerfortpflanzung#
			# Mehrere_fehlerbehaftete_Gr.C3.B6.C3.9Fen
		sd_vHor<-	(sub$sdN.m/sub$N.m + sub$sdE.m/sub$E.m)[-1]
		sd_v3D<-	(sub$sdN.m/sub$N.m + sub$sdE.m/sub$E.m + sub$sdh.m/sub$h.m)[-1]
		sd_zen_sin<-	apply(matrix(cbind(sin(sub[,"zen.deg_"&agg]+sub[,"zen.deg_sd"&substr(agg,4,4)]),
						sin(sub[,"zen.deg_"&agg]-sub[,"zen.deg_sd"&substr(agg,4,4)]) ),ncol=2),1,FUN=sd,na.rm=T)/
						abs(sin(sub[,"zen.deg_"&agg]))
		azi<-sub[,"azi.deg_"&agg]*pi/180;azi.sd<-sub[,"azi.deg_sd"&substr(agg,4,4)]*pi/180#convert to radians
		zen<-sub[,"zen.deg_"&agg]*pi/180;zen.sd<-sub[,"zen.deg_sd"&substr(agg,4,4)]*pi/180#convert to radians
		sd_azi_sin<-	apply(matrix(cbind(sin(azi+azi.sd),sin(azi-azi.sd)),ncol=2),1,FUN=sd,na.rm=T)/
						abs(sin(azi))					
		sd_azi_cos<-	apply(matrix(cbind(cos(azi+azi.sd),cos(azi+azi.sd) ),ncol=2),1,FUN=sd,na.rm=T)/
						abs(cos(azi))			
	 	sd_zen_sin<-	apply(matrix(cbind(sin(zen+zen.sd),sin(zen-zen.sd)),ncol=2),1,FUN=sd,na.rm=T)/
						abs(sin(zen))	
		sd_inc <- 		sd_azi_sin+sd_azi_cos+sd_zen_sin
		sd_vHor.f<-	sd_vHor + sd_inc[-1]
		sd_v3D.f<- sd_v3D +	sd_inc[-1]
		
		#make data-frame with velocities
		rate.now<-data.frame(generation_time=sub$generation_time[-1],
				position=sub$position[-1],
				device_id=rep(as.factor(dev.change$device_id[epi]), length(dTime)),
				velHor= dHor/dTime,
				velHor.f= dHor.f/dTime,
				vel3D= d3D/dTime,
				vel3D.f=d3D.f/dTime,
		    velHor_azi=velHor_azi,
		    velHor_azi.f=velHor_azi.f,              
				mast.h=sub$mast.h[-1],
				sd_vHor=sd_vHor,
				sd_v3D=sd_v3D,
				sd_vHor.f=sd_vHor.f,
				sd_v3D.f=sd_v3D.f)
		
		rate.s<- rbind(rate.s,rate.now)	
	}#end epi-loop
	
  #correct azimuth of velocity
  rate.s$velHor_azi[rate.s$velHor_azi<0] <- 360+ rate.s$velHor_azi[rate.s$velHor_azi<0]
	rate.s$velHor_azi.f[rate.s$velHor_azi.f<0] <- 360+ rate.s$velHor_azi.f[rate.s$velHor_azi.f<0]
  
  
  
#		#stats over period
#		stats<- NULL
#		for(width.d in c(2,7,14,30,60,100)){
#		rate.sd<- xs.ts.aggregate(data=rate.now,width=86400*width.d,funct="sd") 	
#		rate.mean<- xs.ts.aggregate(data=rate.now,width=86400*width.d,funct="sd")
#		stats.now<- data.frame(
#					position=gps.inc$position[1],
#					agg=agg, by=by,
#					mast.h=gps.inc$mast.h[1],
#					width.d=width.d,
#					mean.Hor= mean(rate.mean$velHor,na.rm=T),
#					mean.Hor.f= mean(rate.mean$velHor.f,na.rm=T),
#					mean.3D= mean(rate.mean$vel3D,na.rm=T),
#					mean.3D.f= mean(rate.mean$vel3D.f,na.rm=T),
#					sd.Hor=sd(rate.sd$velHor,na.rm=T),
#					sd.Hor.f = sd(rate.sd$velHor.f,na.rm=T),
#					sd.3D=sd(rate.sd$vel3D,na.rm=T),
#					sd.3D.f = sd(rate.sd$vel3D.f,na.rm=T),
#					CV.Hor=sd(rate.sd$velHor,na.rm=T)/mean(rate.mean$velHor,na.rm=T),
#					CV.Hor.f=sd(rate.sd$velHor.f,na.rm=T)/mean(rate.mean$velHor.f,na.rm=T),
#					CV.3D=sd(rate.sd$vel3D,na.rm=T)/mean(rate.mean$vel3D,na.rm=T),
#					CV.3D.f=sd(rate.sd$vel3D.f,na.rm=T)/mean(rate.mean$vel3D.f,na.rm=T)
#					)
#		stats<- rbind(stats, stats.now)
#		}

#		Statistics
		
		#get subset in winter-period
		sub<-subset(rate.s,generation_time>=as.POSIXct("2011-12-01") & 
				generation_time<=as.POSIXct("2012-03-01") )


		stats<-data.frame(position=gps.inc$position[1],
					agg=agg, by=by,
					mast.h=gps.inc$mast.h[1],
					mean.Hor= mean(rate.s$velHor,na.rm=T),
					mean.Hor.f= mean(rate.s$velHor.f,na.rm=T),
					mean.3D= mean(rate.s$vel3D,na.rm=T),
					mean.3D.f= mean(rate.s$vel3D.f,na.rm=T),
					sd.Hor=sd(rate.s$velHor,na.rm=T),
					sd.Hor_w=sd(sub$velHor,na.rm=T),
					sd.Hor.f = sd(rate.s$velHor.f,na.rm=T),
					sd.Hor.f_w = sd(sub$velHor.f,na.rm=T),
					sd.3D=sd(rate.s$vel3D,na.rm=T),
					sd.3D.f = sd(rate.s$vel3D.f,na.rm=T),
					sd_vergl = sd(rate.s$velHor.f,na.rm=T)/sd(rate.now$velHor,na.rm=T),
					ind = sd(rate.s$velHor.f,na.rm=T)/sd(rate.now$velHor,na.rm=T)<1,
					sd_vergl_w = sd(sub$velHor.f,na.rm=T)/sd(sub$velHor,na.rm=T),
					ind_w = sd(sub$velHor.f,na.rm=T)/sd(sub$velHor,na.rm=T)<1
					)



	#plot
#pdf(file=paste(plot.path,"pos-",pos,"_simple_velocity.pdf",sep=""))	
#	xs.gps.rate.simple.plot(rate.simple=rate.s,dev.change)
#dev.off()
	
	return(rate.simple=list(rate=rate.s, stats=stats))
}

#==============================================================================
# PLOT RATE (DAILY VALUES
#
# calculates daily rates
# Plot rate and sd. Indicates device changes.
#
# Only process one position at a time!!
#
# INPUT daily data frame with:
# 	- generation_time
#	- position
#	- device_id
#	- rate
#	- rate.sd
#	- TYPE: "INC", "COM", "GPS1D", "GPS2D"
#
# Author: Vanessa
#==============================================================================
#-- add arrows to plots ---------------------------------------------
xs.plot.arrows<-function(dev.change,yrange) {
	#make arrows
	ay<-yrange[2]+diff(yrange)/50
	for (d in 1:length(dev.change[,1])) {
		#arrows
		arrows(dev.change$dbeg[d],ay,dev.change$dend[d],ay,code=3,length=0.1) 
		#position numbers
		text(mean(c(dev.change$dbeg[d],dev.change$dend[d])),ay+diff(yrange)/25,dev.change$device_id[d])
		#vertical guide lines
		lines(rep(dev.change$dend[d],2),c(0,yrange[2]),lwd=0.8)
		lines(rep(dev.change$dbeg[d],2),c(0,yrange[2]),lwd=0.8)
	}
}

#-- end arrows -------------------------------------------------------
xs.plot.rate.daily<-function(rate,dev.change, mydev,pdf.file,TYPE=TYPE,
		width=20, height=10,time_beg="2010-12-01", time_end="2012-09-01"){
	#if no data is there to plot, then exit
	valid<-TRUE
	if (is.null(rate)==TRUE) valid<-FALSE
	if (length(rate$position) <= 0) valid<-FALSE
	if (valid == FALSE) {
		print('No data to plot.')
		return(NULL)
	}
	
	#make good label
	site<-" ("&mydev$Label[1]&" | "&mydev$X[1]&")"
	
	#graphical parameters
	pch<-20
	NDateTick<-9	
	
	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-width*CTI
	height<-height*CTI
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	DateTicks<-pretty(xlim,NDateTick)
	
	#get daily values, make nice title
	if(TYPE=="GPS3D") {
		rate<- xs.rate.daily(rate, TYPE="GPS2D")
		ylab<-"velocity [m/d]"; 
		main<-"3D-Velocity (at antenna) \nfor position "&mydev$Position[1]&site}
	if(TYPE=="GPS2D") {
		rate<- xs.rate.daily(rate, TYPE="GPS2D")
		ylab<-"velocity [m/d]";
		main<-"Velocity (at antenna) \nfor position "&mydev$Position[1]&site}
	if(TYPE=="GPSE"){
		rate<- xs.rate.daily(rate, TYPE="GPS1D") 
		ylab<-"velocity [m/d]";
		main<-"Velocity (at antenna) in E-direction \nfor position "&mydev$Position[1]&site}
	if(TYPE=="GPSN"){
		rate<- xs.rate.daily(rate, TYPE="GPS1D") 
		ylab<-"velocity [m/d]";
		main<-"Velocity (at antenna) in N-direction \nfor position "&mydev$Position[1]&site}
	if(TYPE=="COM"){
		rate<- xs.rate.daily(rate, TYPE="GPS2D")
		main<-"Velocity (at foot) \nfor position "&mydev$Position[1]&site
		ylab<-"velocity [m/d]";}
	if(TYPE=="COM3D"){
		rate<- xs.rate.daily(rate, TYPE="GPS2D")
		main<-"3D-Velocity (at foot) \nfor position "&mydev$Position[1]&site
		ylab<-"velocity [m/d]";}
	if(TYPE=="INC"){
		rate<- xs.rate.daily(rate, TYPE="INC")
		main<-"Rate of change \nfor position "&mydev$Position[1]&site; 
		ylab="[deg/day]";}
	
	#sort data
	rate<-rate[with(rate, order(date)), ]
	
	#make ylim
	ymax<-max(abs(rate$rate)+abs(rate$rate.sd), na.rm=T)
	ylim=c(0,ymax*1.1)
	
	#output to PDF
	if(is.null(pdf.file)==0)pdf(file=pdf.file,width,height,onefile=TRUE)
	
	#PLOT
#	#std
	plot(y=(abs(rate$rate)+rate$rate.sd), x=rate$date, pch="-", type="n",
			lwd=1,col="darkgrey",ylim=ylim,axes=F, xlab="", xlim=xlim,
			ylab=ylab,main=main)
	segments(x0=rate$date,x1=rate$date, 
		y0=(abs(rate$rate)-rate$rate.sd), y1=(abs(rate$rate)+rate$rate.sd),
		col="lightgrey",lwd=2)

	#QUANTILE
#	plot(y=(abs(rate$rate)+rate$rate.sd), x=rate$date, pch="-", type="n",
#			ylim=ylim,axes=F, xlab="", xlim=xlim,
#			ylab=ylab,main=main)
#	segments(x0=rate$date,x1=rate$date, 
#		y0=abs(rate$rate.q1), y1=abs(rate$rate.q2),
#		col="lightgrey",lwd=2)
			
	
	#rate
	lines(y=abs(rate$rate), x=rate$date, lwd=0.3, lty=3,
		ylim=ylim, cex=0.2,col="darkblue")
	points(y=abs(rate$rate), x=rate$date, 
			pch=16, ylim=ylim, cex=0.3,col="darkblue")

	#indicate non-significant data-points
	points(y=abs(rate$rate[rate$fwd=="none"]), x=rate$date[rate$fwd=="none"], pch=16, 
			cex=0.4, col="lightblue2")
	

	
	#make axis	
	axis(2, col="black"); 
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"))
	box(); grid()
	
	#add arrows
	yrange<- range(abs(rate$rate)+abs(rate$rate.sd), na.rm=T)#ylim for arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,yrange)
	
	
	if(TYPE=="INC"){#for INC: add rate for azi
		#std
		plot(y=(abs(rate$rate_azi)+rate$rate_azi.sd), x=rate$date, type="l", 
				lwd=1,col="darkgrey",ylim=ylim,axes=F, xlab="", xlim=xlim,
				ylab=ylab,main=main)
		points(y=(abs(rate$rate_azi)+rate$rate_azi.sd), x=rate$date, pch=16, 
				col="darkgrey",cex=0.25)
		lines(y=(abs(rate$rate_azi)-rate$rate_azi.sd), x=rate$date, type="l", 
				lwd=1,col="darkgrey")
		points(y=(abs(rate$rate_azi)-rate$rate_azi.sd), x=rate$date, pch=16, 
				col="darkgrey",cex=0.25)
		#rate
		lines(y=abs(rate$rate), x=rate$date, lwd=1.5, ylim=ylim, cex=0.2,col="darkblue")
		#indicate not significant data-points
		points(y=abs(rate$rate_azi[rate$fwd=="none"]), x=rate$date[rate$fwd=="none"], pch=16, 
				type="p",cex=0.3, col="lightblue2")
		#make axis	
		axis(2, col="black"); box(); grid()
		axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"))
		if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,
					range(rate$rate_azi,na.rm=T))#add arrows
	}
	if(is.null(pdf.file)==0)dev.off()
}

#==============================================================================
# PLOT AZIMUTH of RATE (DAILY VALUES)
#
# calculates daily rates
# Plot rate and sd. Indicates device changes.
#
# Only process one position at a time!!
#
# INPUT daily data frame with:
# 	- generation_time
#	- position
#	- device_id
#	- rate.azi
#
# Author: Vanessa
#==============================================================================
xs.plot.rate.azi<-function(rate=gps.rate.2D,dev.change, mydev,pdf.file=NULL
		,time_beg=plot_beg,time_end=plot_end,width=20, height=10){
	#if no data is there to plot, then exit
	valid<-TRUE
	if (is.null(rate)==TRUE) valid<-FALSE
	if (length(rate$azi_en) <= 0) valid<-FALSE
	if (valid == FALSE) {
		print('No data to plot.')
		return(NULL)
	}
	
	#get daily values
	rate<- xs.rate.daily(rate, TYPE="GPS2D")
	
	#make good label
	site<-" ("&mydev$Label[1]&" | "&mydev$Sensor.Type[1]&")"
	
	#graphical parameters
	pch<-20
	NDateTick<-9	
	
	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-width*CTI
	height<-height*CTI
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	DateTicks<-pretty(xlim,NDateTick)
	
	#output to PDF
	if(is.null(pdf.file)==0)pdf(file=pdf.file,width,height)
	
	#plot data
	plot(y=rate$rate.azi, x=rate$date,  type="p", pch=16,lwd=1,cex=0.6,
			col="darkblue",ylim=c(0,380),xlim=xlim,axes=F, xlab="",  
			ylab="azimuth of rate [deg from North]",
			main="Azimuth of rate for position "&mydev$Position[1]&site)
	lines(y=rate$rate.azi, x=rate$date,col="darkblue",lwd=0.2)	
	#indicate not significant data-points
	points(y=rate$rate.azi[rate$fwd=="none"], x=rate$date[rate$fwd=="none"], pch=16, 
	       type="p",cex=0.7, col="lightblue2")
	#add axis
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"));
	axis(2, col="black"); box(); grid()
	#add arrows for device-change
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,yrange=c(0,360) )#add arrows
	
	if(is.null(pdf.file)==0)dev.off()
}

#==============================================================================
# FUNCTION TO CALCULATE ALL RATES AND PLOT THEM
#
# plots raw inclinometer values
# plots daily values of x and y in degrees
# indicates change in device-id
#
# RETURNS: PDF of plot file
#
# Author: Vanessa Wirz
#==============================================================================
xs.plot.rate.ALL<-function(gps.data,inc.daily, pos,dev.change,snr,snr.com,by,rm.outlier,realizations,
		plot_beg,plot_end,data.wd,plot.path, width=20, height=10){
	#save GPS data
#	save(gps.data, file=data.wd&"pos"&as.character(pos)&"_gps_data.Rdata")
	
	#plot DISTANCE (antenna)
 	xs.plot.dist(gps.data,dev.change,paste(plot.path,"distance/pos-",pos,"GPSdis.pdf",sep=""),
 			mydev, type="GPS", time_beg=plot_beg,time_end=plot_end)		
	
	#calculate rate of change
	# 1D
	gps.rate.E=NULL; gps.rate.N=NULL
#	gps.rate.E<-xs.gps.rate.lm.1D(gps.data, component="E.m", dev.change, snr=snr
#			,rm.outlier=rm.outlier,realizations=realizations, pdf.file=NULL)
#			
#	gps.rate.N<-xs.gps.rate.lm.1D(gps.data, component="N.m", dev.change, snr=snr
#			,rm.outlier=rm.outlier,realizations=realizations,pdf.file=NULL)
#	
#	gps.rate.h<-xs.gps.rate.lm.1D(gps.data, component="h.m", dev.change, snr=snr
#			,rm.outlier=rm.outlier,realizations=realizations,pdf.file=NULL)

	#2D
	gps.rate.2D<-NULL
	gps.rate.2D<- xs.gps.rate.lm(data=gps.data, dev.change, snr=snr,rm.outlier=rm.outlier,
			realizations=realizations, TYPE="GPS",DIM="2D")

	#3D
	gps.rate.3D<-NULL
#	gps.rate.3D<- xs.gps.rate.lm(data=gps.data, dev.change, snr=snr,rm.outlier=rm.outlier,
#			realizations=realizations, TYPE="GPS",DIM="3D")

	#COM
	com.rate.2D.med<-NULL;com.rate.3D<-NULL
 	if(is.null(inc.daily)==0){
		if(inc.daily$position[1]== pos){
 		gps.inc<- merge(gps.data, inc.daily, by = "generation_time",sort = TRUE,incomparables = NA)
		names(gps.inc)[2]<- "position"
		
		#calculate GPS-foot positions
		foot<-xs.com.pre(E.m=gps.inc$E.m,N.m=gps.inc$N.m,h.m=gps.inc$h.m,
				azi.deg=gps.inc$azi.deg_med,zen.deg=gps.inc$zen.deg_med,mast.h=gps.inc$mast.h)
		gps.foot<-cbind(gps.inc,foot)
		
		#plot DISTANCE (foot)
		xs.plot.dist(gps.foot,dev.change,paste(plot.path,"distance/pos-",pos,"GPSdis_foot.pdf",sep=""),
				mydev, type="FOOT", time_beg=plot_beg,time_end=plot_end)	
		
		#calculate rate

#		com.rate.2D.med<- xs.gps.rate.lm(data=gps.inc, dev.change, snr=snr.com,rm.outlier=rm.outlier,
#				realizations=realizations, TYPE="COM",DIM="2D",agg="med")
#
#		com.rate.3D<- xs.gps.rate.lm(data=gps.inc, dev.change, snr=snr.com,rm.outlier=rm.outlier,
#				realizations=realizations, TYPE="COM",DIM="3D",agg="med")
	}
	if(inc.daily$position[1]!= pos)"wrong position"
	}
	
	#save data
gps.rate<- list(E=gps.rate.E, N=gps.rate.N, dim2=gps.rate.2D,dim3=gps.rate.3D,
		com2med=com.rate.2D.med,com3d=com.rate.3D) 
	

 	#plot RATE
 	pdf(file=paste(plot.path,"pos-",pos,"_GPS_mc",realizations, #pos,
 			"_snr",snr,".pdf",sep=""),width=0.393700787*width,
 			height=0.393700787*height,onefile=TRUE)	
 		#1D
#  		xs.plot.rate.daily(rate=gps.rate.E,dev.change, mydev,TYPE ="GPSE",pdf.file=NULL
#  		,time_beg=plot_beg,time_end=plot_end) 
#  		xs.plot.rate.daily(rate=gps.rate.N,dev.change, mydev,TYPE ="GPSN",pdf.file=NULL
#  				,time_beg=plot_beg,time_end=plot_end)
 		
 		#2D
 		xs.plot.rate.daily(rate=gps.rate.2D,dev.change, mydev,TYPE ="GPS2D",pdf.file=NULL
 				,time_beg=plot_beg,time_end=plot_end)
 		#3D
# 		xs.plot.rate.daily(rate=gps.rate.3D,dev.change, mydev,TYPE ="GPS3D",pdf.file=NULL
# 		,time_beg=plot_beg,time_end=plot_end)
 
# 		if(is.null(com.rate.2D.med)==0){
# 			xs.plot.rate.daily(rate=com.rate.2D.med,dev.change, mydev, TYPE="COM"
# 					,pdf.file=NULL,time_beg=plot_beg,time_end=plot_end)
# 			xs.plot.rate.daily(rate=com.rate.3D,dev.change, mydev, TYPE="COM3D",pdf.file=NULL
# 			,time_beg=plot_beg,time_end=plot_end)
# 		}
 		
 		#add rate.azi
 		xs.plot.rate.azi(rate=gps.rate.2D,dev.change, mydev,pdf.file=NULL,time_beg=plot_beg,
 				time_end=plot_end)
# 		xs.plot.rate.azi(rate=com.rate.2D.med,dev.change, mydev,pdf.file=NULL,time_beg=plot_beg,
# 				time_end=plot_end)
 
 	dev.off()
	return(gps.rate<- list(E=gps.rate.E, N=gps.rate.N, dim2=gps.rate.2D,dim3=gps.rate.3D,
					com2med=com.rate.2D.med,com3d=com.rate.3D) )
}




#==============================================================================
# PLOT CUMULATIVE CHANGE (INCLINATION OR POSITION) 
#
# Detects device changes.
#
# Only process one position at a time!!
#
# INPUT data frame with daily values of:
# 	- generation_time
#	- position
#	- device_id
#   => inclination data or position data
#
#   => use type=c("inc","gps","com") to distinguish data plot type
#
# RETURNS: PDF of plot file
#
# Author: Stephan Gruber
#==============================================================================
xs.plot.dist<-function(data, dev.change, pdf.file, mydev, type="INC", width=16, height=8,
		               time_beg="2010-12-01", time_end="2013-01-01") {
	#if no data is there to plot, then exit
	valid<-TRUE
	if (is.null(data)==TRUE) valid<-FALSE
	if (length(data$position) <= 0) valid<-FALSE
	if (valid == FALSE) {
		print('No data to plot.')
		return(NULL)
	}
	#sort data
	data<-data[with(data, order(generation_time)), ]
	
	#make good label
	site<-" ("&mydev$Label[1]&" | "&mydev$Sensor.Type[1]&")"

	#graphical parameters
	pch<-20
	NDateTick<-9
	
	#distinguish plot types
	if (toupper(type == "INC"))  { #TILTING
		Dmain<-"Total rotation \nfor position "&mydev$Position[1]&site
		Dlab<-"Great-circle distance [deg]"
		Xlab<-"Inclination as zenith angle [deg]"
		Xmain <-"Inclination for position "&mydev$Position[1]&site
		Ylab  <-"Azimuth [deg, clockwise from N, downlooking]"
		Ymain <-"Azimuth for position "&mydev$Position[1]&site
		XYmain<-"Inclination and Azimuth for position "&mydev$Position[1]&site
		#get plotting quantities
		D<-xs.inc.total(data, dev.change) #total distance
		X<-data$zen.deg_med
		Y<-data$azi.deg_med
		X.sd<-data$zen.deg_sd
		Y.sd<-data$azi.deg_sd
		col<-"red"
	}
	if (toupper(type == "GPS"))  { #GPS ANTENNA
		Dmain<-"Horizontal displacement (at antenna) \nfor position "&mydev$Position[1]&site
		Dlab<-"Distance [m]"
		Xlab<-"Easting [m CH1903]"
		Xmain <-"Easting for position "&mydev$Position[1]&site
		Ylab  <-"Northing [m CH1903]"
		Ymain <-"Northing for position "&mydev$Position[1]&site
		Zlab  <-"Elevation [m CH1903]"
		Zmain <-"Elevation for position "&mydev$Position[1]&site
		XYmain<-"Easting and Northing for position "&mydev$Position[1]&site
		names(data)[3:5]<- c("Y","X","Z")
		D<-xs.dist.xyz.per_dev(data, dev.change)[, c(1,2)]
		X<-data$X
		Y<-data$Y
		Z<-data$Z
		X.sd<-data$sdN.m
		Y.sd<-data$sdE.m
		Z.sd<-data$sdh.m
		col<-"blue"			 
	}
	if (toupper(type == "FOOT")) { #MAST FOOT TO COMPLETE!!!!
		Dmain<-"Horizontal displacement (at mast foot) \nfor position "&mydev$Position[1]&site
		Dlab<-"Distance [m]"
		Xlab<-"Easting [m CH1903]"
		Xmain <-"Easting (foot) for position "&mydev$Position[1]&site
		Ylab  <-"Northing [m CH1903]"
		Ymain <-"Northing (foot) for position "&mydev$Position[1]&site
		XYmain<-"Easting and Northing (foot) for position "&mydev$Position[1]&site
		Zlab  <-"Elevation [m CH1903]"
		Zmain <-"Elevation (foot) for position "&mydev$Position[1]&site 
		names(data)[29:31]<- c("X","Y","Z")
		D<-xs.dist.xyz.per_dev(data, dev.change)[, c(1,2)]
		X<-data$X
		Y<-data$Y
		Z<-data$Z
		X.sd<-0
		Y.sd<-0
		Z.sd<-0
		col<-"green"
	print("add standard deviations -> how to calculate???")
	}
	
	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-width*CTI
	height<-height*CTI
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	DateTicks<-pretty(xlim,NDateTick)
	
	#output to PDF
	pdf(file=pdf.file,width,height,onefile=TRUE)	
	
	#DISTANCE -----------------
	plot(D[,1],D[,2],type="p",col=col,pch=pch,xaxt="n",
		 xlim=xlim,ylim=c(0,max(D[,2],na.rm=T)*1.1),xlab="date",ylab=Dlab,main=Dmain)
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"));grid(ny=NULL,nx=0)
	#add arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,range(D[,2],na.rm=T))
		
	#SCATTER X/Y --------------
	plot(X,Y,type="p",col=col,pch=pch,
		 xlab=Xlab,ylab=Ylab,main=XYmain); grid()
	
	#X ------------------------
	ylim.x=range(c(X+X.sd,X-X.sd), na.rm=T)
	plot(data$generation_time,X,type="p",col=col,pch=pch,xaxt="n",
			xlim=xlim,ylim=c(ylim.x[1],ylim.x[2]+diff(ylim.x)*0.1),
			xlab="date",ylab=Xlab,main=Xmain)
	lines(data$generation_time,y=X+X.sd, col="grey");lines(data$generation_time,y=X-X.sd, col="grey")
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"));grid(ny=NULL,nx=0)
	

	#add arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,range(c(X+X.sd,X-X.sd),na.rm=T) )
	
	#Y ------------------------
	ylim.y=range(c(Y+Y.sd,Y-Y.sd), na.rm=T)
	plot(data$generation_time,Y,type="p",col=col,pch=pch,xaxt="n",
			xlim=xlim,ylim=c(ylim.y[1],ylim.y[2]+diff(ylim.y)*0.1),
			xlab="date",ylab=Ylab,main=Ymain)
	lines(data$generation_time,y=Y+Y.sd, col="grey");lines(data$generation_time,y=Y-Y.sd, col="grey")
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"));grid(ny=NULL,nx=0)
	#add arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,range(c(Y+Y.sd,Y-Y.sd),na.rm=T))
	
	#Z ------------------------
	if (toupper(type != "INC"))  {
	ylim.z=range(c(Z+Z.sd,Z-Z.sd), na.rm=T)
	plot(data$generation_time,Z,type="p",col=col,pch=pch,xaxt="n",
			xlim=xlim,ylim=c(ylim.z[1],ylim.z[2]+diff(ylim.z)*0.1),
			xlab="date",ylab=Zlab,main=Zmain)
	lines(data$generation_time,y=Z+Z.sd, col="grey");lines(data$generation_time,y=Z-Z.sd, col="grey")
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"));grid(ny=NULL,nx=0)
	#add arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,range(c(Z+Z.sd,Z-Z.sd),na.rm=T))
	}
	
	
	dev.off() #end PDF	
	
}#-----------------------------------------------------------------------------



#==============================================================================
# PLOT VARIOUS GPS-RATES IN ONE FILE 
#
# INPUT:
#   - TYPE: "setting", "pos" or "pos&setting" (defines what files to select)
#   - TYPERATE: "GPS2D", "GPS3D" or "COM2D"
#   - folder: folder where files are stored
#   - pos: numeric (one value or vector of values)
#   - mc: number of MC-simlations used for rate calculation
#   - snr: SNR used for rate calculation
#   - dev.change: data-frame
#   - time_beg/time_end: start- and end-date of plot
#   - width,height: used for pdf
#   - name of pdf-file
#    
#
# RETURNS: PDF of plot file
#
# Author: Vanessa
#==============================================================================
xs.plot.rate.VAR<-function(TYPE,TYPERATE,folder,pos=NULL,mc=NULL,snr=NULL, 
        dev.change=NULL,time_beg="2011-05-01", time_end="2012-08-01",
        width=25, height=15,pdf.file){
  

  if(TYPE=="setting"){my.pattern="mc"&mc&"_snr"&snr;
          main="Velocities with "&mc&" MC-simulations \nand a SNR-ratio of "&snr} 
  if(TYPE=="pos"){my.pattern="pos"&pos; 
          main="Velocities for pos "&pos}
  if(TYPE=="pos&mc"){my.pattern="pos"&pos&"_rate_"&"mc"&mc;
          main="Velocities with "&mc&" MC-simulations \nfor position "&pos}
  if(TYPE=="pos&snr"){my.pattern=glob2rx("pos"&pos&"*snr"&snr&"*");
		  main="Velocities with snr-ratio of "&snr&"\nfor position "&pos}
  if(TYPE=="pos&setting"){my.pattern="pos"&pos&"_rate_"&"mc"&mc&"_snr"&snr&"_by"&by;
		  main="Velocities with "&mc&" MC-simulations and \nsnr-ratio of "&snr}
  
  if(TYPERATE=="GPS2D"){dim="dim2";dailytype="GPS2D";ylab="horizontal vel [m/d]"}
  if(TYPERATE=="GPS3D"){dim="dim3";dailytype="GPS2D";ylab="3D-vel [m/d]"}
  if(TYPERATE=="COM2D"){dim="com2med";dailytype="GPS2D";ylab="horizontal vel [m/d]"}
  if(TYPERATE=="COM2Dm"){dim="com2med2";dailytype="GPS2D";ylab="horizontal vel [m/d]"}
  
  #get file-names
  filenames<-NULL
  for(pat in my.pattern){
  filename<- list.files(folder,pattern=pat)
  filenames<- c(filenames,filename)}

  
  #get ylim
  ymax=0;ymin=10
  for(i in 1:length(filenames)){
	  setwd(folder)
    max.now= abs(max(data.frame(get(load(filenames[i]))[dim])[,dim&".v_en"],na.rm=T))
    min.now= abs(min(data.frame(get(load(filenames[i]))[dim])[,dim&".v_en"],na.rm=T))
    if(max.now>ymax){ymax=max.now}
    if(min.now<ymin){ymin=min.now}
  }
  ylim=c(0,ymax)
  
  #graphical parameters
  pch<-16
  NDateTick<-9	
  
  #adjust width
  CTI<-0.393700787 #cm to inch by multiplication
  width <-width*CTI
  height<-height*CTI
  
  #convert times, make xlim
  time_beg<-as.POSIXct(time_beg)
  time_end<-as.POSIXct(time_end)
  xlim<-c(time_beg,time_end)
  DateTicks<-pretty(xlim,NDateTick)
  
  #define colours
  col=rainbow(length(filenames))
  
  #pdf
  if(is.null(pdf.file)==0)pdf(file=pdf.file, width=width, height=height)
  
  #load all rate and plot them in one plot
  labels<-NULL
  for(i in 1:length(filenames)){
    setwd(folder)
    file<-filenames[i]
    #set label
    if(TYPE=="pos")label<-substr(file,start=12,stop=nchar(file)-6)
    if(TYPE=="setting")label<-substr(file,start=1,stop=5)
	if(TYPE=="pos&setting")label<-substr(file,start=1,stop=nchar(file)-6)
    if(TYPE=="pos&mc")label<-substr(file,start=18,stop=nchar(file)-6)
    if(TYPE=="pos&snr")label<-substr(file,start=12,stop=nchar(file)-11)
    labels<-c(labels,label)
    
    #get rate and calculate daily values
    r=as.data.frame(get(load(filenames[i]))[dim])
    names(r)<-c("time_beg","time_end","position","device_id","v_en","v_en.sd","azi_en","fwd" )
    rate<- xs.rate.daily(rate=r, TYPE=dailytype) 
    
    if(i==1){#plot first rate
		par(mar=c(5.1, 4.1, 4.1, 10.1), xpd=TRUE)
      plot(y=abs(rate$rate), x=rate$date, type="l", 
           lwd=1,col=col[1],ylim=ylim,xlab="", xlim=xlim,axes=F,
           ylab=ylab,main=main)
      #make axis  
      axis(2, col="black"); 
      axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"))
      box(); grid()
      yrange<- c(0, ylim[2]-ylim[2]/20)#ylim for arrows
      if (is.null(dev.change)==FALSE){
        xs.plot.arrows(dev.change,yrange=yrange)} 
      }#end i==1
    
    if(i>=2){#add additional rate to plot
      lines(y=abs(rate$rate), x=rate$date, type="l", 
           lwd=1,col=col[i],ylim=ylim, xlab="", xlim=xlim)   
    }

	#add legend
	if(i==length(filenames)){
	legend(x=par("usr")[2]+par("usr")[2]/2000,y=mean(par("usr")[4]),
			legend=labels,pch="-",col=col,cex=0.7)}
  }#end loop over files

  

  if(is.null(pdf.file)==0)dev.off()
  
}#----------------------------------------------------------------------------
 

#==============================================================================
# PLOT VARIOUS GPS-RATES in BOX-WHISKER_PLOTS
#
# INPUT:
#   - TYPERATE: "GPS2D" "COM2D"
#   - folder: folder where files are stored
#   - pos: numeric (one value or vector of values)
#   - dev.change: data-frame
#   - time_beg/time_end: start- and end-date of plot
#   - width,height: used for pdf
#   - name of pdf-file
#    
#
# RETURNS: PDF of plot file
#
# Author: Vanessa
#==============================================================================
xs.plot.box<-function(folder,pos,outlier=TRUE,pdf.file){
	setwd(folder)
	my.pattern="pos"&pos
	
	#get filenames
	filenames<- list.files(folder,pattern=my.pattern)
	length(filenames)
	
	#
	pdf(pdf.file, width=0.393700787*20, height=0.393700787*30)
	par(mfrow=c(4,2))
	for(filename in filenames){
		data<- get(load(filename))
		snr<- substr(filename,21,21)
		by<- substr(filename,25,25)
		mc<- substr(filename,14,16)
		
		dim2<- xs.rate.daily(rate=data$dim2, TYPE="GPS2D")[,1:4]
		com2med<- xs.rate.daily(rate=data$com2med, TYPE="GPS2D")[,c(1,4)]
		#com2med2<-xs.rate.daily(rate=data$com2med2, TYPE="GPS2D")[,c(1,4)]
		tmp<- c(dim2$rate,com2med$rate )#,com2med2$rate
		rate<-data.frame(date=c(dim2$date,com2med$date),
				type=c(rep("dim2", length(dim2$date)),
						rep("com", length(com2med$date))),
						#rep("com2", length(com2med2$date))),
				rate=tmp)
		boxplot(rate$rate~rate$type,outline=outlier, main="pos-"&pos&" mc-"&mc&" snr-"&snr, 
				ylab="velocity [m/day]")	; grid()
	}
	dev.off()
}



#==============================================================================
# PLOT OVERVIEW OF DB HOLDINGS
#
# 
#
# RETURNS: PDF of plot file
#
# Author: Stephan Gruber
#==============================================================================
xs.plot.overview<-function(np.file, file, time_beg="2010-01-01", 
		                   time_end="2013-01-01") {
	#read nodepositions
	meta<-xs.positions.xls(np.file)
	meta<-subset(meta,substr(Sensor.Type,1,3)=="GPS")
	meta<-subset(meta,is.na(Position)==FALSE)
	
	#number of positions
	npos<-length(meta[,1])
	
	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-32*CTI
	height<-(4+npos*0.6)*CTI
	
	#convert times
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	
	#plot limits
	xlim<-c(time_beg,time_end)
	ylim<-c(0,npos+1)
	
	#output to PDF
	pdf(file=file,width,height)
	
	#margins
	par(mar=c(2,15,2,2) + 0.1)
	
	#make labels
	labels<-NULL
	for (n in 1:nrow(meta)) labels<-rbind(labels, "Pos "& meta$Position[n] &
						                           " ("&meta$Label[n]&" | "&
												   meta$Sensor.Type[n]&")")
	
	datelabel<- seq(from=time_beg, to=time_end, by="4 month")
							   
	#make plot
	plot(xlim,c(-10,-10),xlim=xlim,ylim=ylim,yaxt="n",ylab="",xlab="", axes=F,
		main=paste("X-Sense data status (",as.POSIXlt(Sys.time(), "UTC"),")",sep=""))
	axis(2, las=2, at=1:npos, labels=labels) 
	axis(1, at=datelabel, tick= T, labels= strftime(datelabel,"%Y/%m"), xlim=xlim)
	box()
	
	abline(h=(1:npos+1)-0.5)
		
	legend("topleft", c("GSN: Inclinometer","GSN: GPS raw (qual > 6)","GSN: GPS raw (qual <= 6)","GSN: GPS processed"), inset=.05, title="Colour legend",
			lty=c(1,1,1,1), bg="white", # gives the legend appropriate symbols (lines)
			lwd=c(2.5,2.5,2.5,2.5),col=c("black","red","orange","green")) # gives the legend lines the correct color and width
	
	#loop over positions
	yp<-1
	for (pos in meta$Position) {
		print("Overview plot: processing position: "&pos)
		#select device
		mydev<-subset(meta,Position==pos)
		dev.change<-xs.positions.changes(mydev)
		if (length(mydev[,1])!=1) break #catch empty or non-GPS lines
		
		#INCLINOMETER FROM GSN-----------------------------------------------------
		if (substr(mydev$Sensor.Type,1,10)=="GPS Logger") {
			inc<-xs.inc.ready(pos, dev.change, mydev$mast.o, mydev$mast.h)
			if(is.null(inc)==FALSE) {
				#plot inclinometer
				now<-unlist(subset(inc,is.na(x.deg.med)==FALSE, select=generation_time))
				points(now,rep(yp+0.2,length(now)),pch=15, col=1,cex=0.3)
				#plot device changes
				for (d in 1:length(dev.change[,1])) {
					#vertical guide lines
					lines(rep(dev.change$dend[d],2),c(yp-0.5,yp+0.5),lwd=0.8)
					lines(rep(dev.change$dbeg[d],2),c(yp-0.5,yp+0.5),lwd=0.8)
				}
			} else {
				inc       <-NULL
			}
		}
		
		#GPS RAW DATA STATISTICS---------------------------------------------------
		gps.raw<-xs.gps.raw.gsn(pos,"01/12/2010+12:00","01/09/2012+12:00")
		
		#GPS PROC. DATA STATISTICS---------------------------------------------------
		gps.pro<-xs.gps.pro.gsn(pos)
	
		#plot GPS stats, good
		if (is.null(gps.raw)==FALSE) {
			now<-unlist(subset(gps.raw,measurement_quality>6, select=generation_time))
			points(now,rep(yp,length(now)),pch=15, col=2,cex=0.3)
		}	
		#plot GPS stats, medium
		if (is.null(gps.raw)==FALSE) {
			now<-unlist(subset(gps.raw,measurement_quality<=6, select=generation_time))
			points(now,rep(yp,length(now)),pch=15, col="orange",cex=0.3)
		}	
		
		#plot GPS solutions
		if (is.null(gps.pro)==FALSE) {
			now<-unlist(subset(gps.pro,is.na(n)==FALSE, select=generation_time))
			points(now,rep(yp-0.2,length(now)),pch=15, col=3,cex=0.3)
		}
		yp<-yp+1	
	}#end for 
	dev.off() #end PDF	
}#----------------------------------------------------------------------------



#==============================================================================
# PROCESS DATA
#
# 
#
# RETURNS: rate of change for gps and inclination
#
# Author: vanessa
#==============================================================================
xs.process.position<- function(pos, realizations, snr, snr.com=snr,
    mast.h=1, sd.mast.o=0,rm.outlier=FALSE){
    
    #load input.data
    input.data<- get(load(file="~/in/input.data.Rdata"))

	#get data from input file
	dev.change	<- subset(as.data.frame(input.data$meta.changes),position==pos)
	mydev		<- subset(as.data.frame(input.data$meta.data),Position==pos)
	inc.glob	<- subset(as.data.frame(input.data$inc.glob),position==pos)
	gps.data	<- subset(as.data.frame(input.data$gps.data),position==pos)
	
    inc.rate<- NULL;com.rate.2D<-NULL;com.rate.3D<-NULL
	if (dim(inc.glob)[1]>1) {
	#add parameters mast.h, sd.mast.o
	inc.glob$mast.h <- rep(mast.h, length(inc.glob$mast.h))
	inc.glob$sd.mast.o <- rep(sd.mast.o, length(inc.glob$sd.mast.o))
	
	#get hourly and daily values for inc
	inc.daily 	<- xs.inc.pre.med(inc.glob,dev.change,threshold=10,by=1)
	inc.daily$sd.mast.o <- rep(sd.mast.o, dim(inc.daily)[1])
	inc.glob.h<- xs.ts.aggregate(inc.glob,width=3600,"median")
	
	#merge gps and inc-data and calculate position of gps-foot
	gps.inc<- merge(gps.data, inc.daily, by = "generation_time",sort = TRUE,incomparables = NA)
	names(gps.inc)[2]<- "position"
	foot<-xs.com.pre(E.m=gps.inc$E.m,N.m=gps.inc$N.m,h.m=gps.inc$h.m,
			azi.deg=gps.inc$azi.deg_med,zen.deg=gps.inc$zen.deg_med,mast.h=gps.inc$mast.h)
	gps.foot<-cbind(gps.inc,foot)
	
	#calculate rate of change inclination
	inc.rate<- xs.inc.rate.lm(inc.glob.h[1:40,], dev.change, snr=snr,realizations=realizations)
	
	com.rate.2D<- xs.gps.rate.lm(data=gps.inc, dev.change, snr=snr.com,rm.outlier=rm.outlier,
			realizations=realizations, TYPE="COM",DIM="2D",agg="med")
	
	com.rate.3D<- xs.gps.rate.lm(data=gps.inc, dev.change, snr=snr.com,rm.outlier=rm.outlier,
			realizations=realizations, TYPE="COM",DIM="3D",agg="med")
	}
	#GPS 1D
	gps.rate.E<-NULL;gps.rate.N<-NULL;gps.rate.h<-NULL
	gps.rate.E<-xs.gps.rate.lm.1D(gps.data, component="E.m", dev.change, snr=snr
			,rm.outlier=rm.outlier,realizations=realizations, pdf.file=NULL)
			
	gps.rate.N<-xs.gps.rate.lm.1D(gps.data, component="N.m", dev.change, snr=snr
			,rm.outlier=rm.outlier,realizations=realizations,pdf.file=NULL)
	
	gps.rate.h<-xs.gps.rate.lm.1D(gps.data, component="h.m", dev.change, snr=snr
			,rm.outlier=rm.outlier,realizations=realizations,pdf.file=NULL)
	
	#GPS 2D
	gps.rate.2D<-NULL
	gps.rate.2D<- xs.gps.rate.lm(data=gps.data, dev.change, snr=snr,rm.outlier=rm.outlier,
			realizations=realizations, TYPE="GPS",DIM="2D")
	
	#3D
	gps.rate.3D<-NULL
	gps.rate.3D<- xs.gps.rate.lm(data=gps.data, dev.change, snr=snr,rm.outlier=rm.outlier,
			realizations=realizations, TYPE="GPS",DIM="3D")

	rate<- list(inc.rate=inc.rate,
			gps.rate.E=gps.rate.E,gps.rate.N=gps.rate.N,gps.rate.h=gps.rate.h,
			gps.rate.2D=gps.rate.2D,gps.rate.3D=gps.rate.3D,
			com.rate.2D=com.rate.2D,com.rate.3D=com.rate.3D)

	save(rate,file=data.wd&"pos-"&pos&"_rates_MC"&realizations&"_snr"&snr&"_h"&mast.h&
    "sd_o"&sd.mast.o&".Rdata")
    
	return (rate)
}


#---------------------------------------------------------------------------

#==============================================================================
# PLOT x y values (raw and processed) of inclination-measurements
#
# plots raw inclinometer values
# plots daily values of x and y in degrees
# indicates change in device-id
#
# RETURNS: PDF of plot file
#
# Author: Vanessa Wirz
#==============================================================================
xs.plot.incl.DN <- function(pos, incl.daily, inc.raw, time_beg=plot_beg,
		time_end=plot_end, dev.change=dev.change,width=16, height=8){

	#make good label
	site<-" ("&mydev$Label[1]&" | "&mydev$Sensor.Type[1]&")"	

	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-width*CTI
	height<-height*CTI
	
	#graphical parameters
	pch<-20
	NDateTick<-9
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	DateTicks<-pretty(xlim,NDateTick)	
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	DateTicks<-pretty(xlim,NDateTick)
	
	
	#output pdf
	pdf(file=paste(plot.path,"distance/plot_INCxy_pos-",pos,".pdf", sep=""), width=8, height=5)
	
	#plot x/y-RAW values
	main.now<- "Raw inclinometer measurements for position "&mydev$Position[1]&site
	plot(x=inc.raw$generation_time, inc.raw$x.DN, main=main.now,
			pch=16, axes=F, ylab="DN",xlab="",ylim=c(-10000, 13000))
	points(x=inc.raw$generation_time,inc.raw$y.DN, pch=16,col="red")
	axis(2, col="black"); 
	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"))
	box()
	legend("topright", legend=c("x.raw","y.raw"), pch=16, col=c("black", "red"))
	segments(y0=-9900, y1=0, x0=dev.change$dbeg) 
	segments(y0=-9900, y1=0, x0=dev.change$dend)
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,yrange=c(-10000, 10000))
	
	
#	#plot x/y-DEG values
# 	yrange<-range(c(inc.daily$zen.deg_med,inc.daily$zen.deg_med2))
# 	main.now<- "Daily inclinometer values for position "&mydev$Position[1]&site
# 	plot(inc.daily$generation_time, inc.daily$zen.deg_med, main=main.now, 
# 			pch=16, axes=F, ylab="inclination [deg]",xlab="", ylim=yrange )
# 	axis(2, col="black"); 
# 	axis(1, at=DateTicks,labels=format(DateTicks, "%y.%m.%d"))
# 	points(inc.daily$generation_time, inc.daily$zen.deg_med2,pch=16,col="red")
# 	points(inc.daily$generation_time, inc.daily$x.deg_mu,pch=4,col="grey",cex=0.3)
# 	points(inc.daily$generation_time, inc.daily$y.deg_mu,pch=4,col="orange",cex=0.3)
# 	legend("topleft", legend=c("x.med","y.lm","x.mu","y.mu"), pch=c(16,16,4,4), 
# 			col=c("black", "red", "grey","orange"), ncol=2)
# 	ymax=max(c(inc.daily$x.deg.lm,inc.daily$y.deg.lm))
# 	ymin=min(c(inc.daily$x.deg.lm,inc.daily$y.deg.lm))
# 	segments(y0=ymin, y1=0, x0=dev.change$dbeg); 
# 	segments(y0=ymin, y1=0, x0=dev.change$dend);
# 	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,yrange)
# 	box()	
	
dev.off()
} #----------------------------------------------------------------------------




#------------------------------------------------------------------------------
## Get max generation time for GPS positions
#function(position, time_beg="01/01/2010+12:00", 
#         time_end="01/01/2013+13:00") {
#  #make request
#  virtual_sensor<-"dirruhorn_gps_raw__mapped"
#  fields<-"position,device_id,generation_time,"&
#    "gps_sats,measurement_quality,signal_strength"
#  data<-gsn.query(virtual_sensor, position, fields=fields, aggregation=24, 
#                  time_beg=time_beg, time_end=time_end) 
#  max_time<- max(data$generation_time)
#  #return result as list of data frames
#  return(data)
#}

#==============================================================================
# Identify GAPS in GSN-data
# INPUT: 
#   - data fame with a column "generation_time"
#   - gap.d: number of days to identify gaps
#
# RETURNS: data frame, with new columns:
#   - pos: position
#   - beginning of data generation-time
#   - end of data generation-time
#   - gap-period (integer)
#   - beginning-date of gap period
#   - end-date of gap period
#
# AUTHORS: Vanessa Wirz
#==============================================================================
#find.gaps <- function(pos, gap.d=3, type="RAW"){
#	#load data
#	if(type=="RAW")data<- get(load(file=data.wd&"pos"&as.character(pos)&"_gps_raw.Rdata"))
#	if(type=="PROC")data<- get(load(file=data.wd&"pos"&as.character(pos)&"_gps_data.Rdata"))
#  date<- data$generation_time[order(data$generation_time)]
#  #find start/end date of data
#  start <- date[1]
#  end <- date[length(date)] 
#  
#  #find all gaps
#  if(type=="RAW") gap <- round(diff(date, lag=1)/24, digits=0) >= gap.d
#  if(type=="PROC") gap <- diff(date, lag=1) >= gap.d
#  gap.beg <- date[gap]
#  gap.end <- date[which(is.element(date, gap.beg)) +1]
#  
#  #make data-frame
#  l<- length(gap.end)
#  gap.period <- seq(1,by=1, length=l)
#  gaps <- data.frame(pos=rep(pos,l), start.date= strftime(rep(start, l), format="%Y-%m-%d"), 
#           end.date=strftime(rep(end,l), format="%Y-%m-%d"), gap.period=as.factor(gap.period), 
#           gab_beg=strftime(gap.beg, format="%Y-%m-%d"), gab_end=strftime(gap.end, format="%Y-%m-%d"))
#  return(gaps) 
#}



##############################################################################################################################################################


# OLD FUNCTIONS


##############################################################################################################################################################


#==============================================================================
# RATE OF ROTATION
#
# Uses Monte-Carlo to propagate uncertainty and then calculates the rate of
# change based on a required signal-to-noise ratio. If this condition is not
# fulfilled, the time lag is increased until it is met. The rate of rotation
# is based on the great-circle distance (angle) between two points.
#
# INPUT inclinometer data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.o
#   - x.deg.med
#   - y.deg.med
#   - x.deg.sd
#   - y.deg.sd
#  ==> further parameters:
#   - snr           (signal-to-noise ratio)
#	- realizations	(number of Monte-Carlo realizations)
#
# RETURNS: new data frame with:
# 	- generation_time		
#	- position
#	- device_id
#	- rot.rate    [deg/day]
#	- rot.rate.sd [deg/day]
# ==> Plot with option type="s" in R to obtain steps.
#
# Author: Stephan Gruber
#==============================================================================
#---AUXILLIARY FUNCTION--------------------------------------------------------
xs.inc.rate.loc<-function(sub, realizations) {
	#get length
	len<-length(sub$x.deg.med)
	
	#make matrices: N rows times R columns
	#add temporally-uncorrelated random noise
	x.deg<-matrix(rep(sub$x.deg.med,realizations)+
					rep(sub$x.deg.sd, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol=realizations,byrow=FALSE)
	y.deg<-matrix(rep(sub$y.deg.med,realizations)+
					rep(sub$y.deg.sd, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol=realizations,byrow=FALSE)
	angle<-matrix(rep(sub$mast.o,   realizations),
			nrow=len,ncol=realizations,byrow=FALSE)
	
	#calculate inc/azi in global system
	loc<-xs.inc.loc2glob(x.deg,y.deg,angle=angle)
	
	#make a matrix of it
	azi<-matrix(loc$azi.deg,nrow=len,ncol=realizations,byrow=FALSE)
	zen<-matrix(loc$zen.deg,nrow=len,ncol=realizations,byrow=FALSE)
	
	#return results
	return(list(azi=azi,zen=zen))
}
#---REAL FUNCTION--------------------------------------------------------------
xs.inc.rate<-function(inc, dev.change, snr=2.5/1,realizations=5000) {
	#make sure data exists
	if (is.null(inc)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	
	#ensure only one position is processed
	if (length(unique(inc$position))>1) return(NA)
	
	#loop over device episodes
	rate<-NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(inc,device_id==dev.change$device_id[epi])
		sub<-subset(sub,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)
		
		#compute matrix of R realizations for N point locations
		loc <-xs.inc.rate.loc(sub, realizations)
		days<-as.numeric(sub$generation_time)/86400
		
		f1<-1 #start with first value
		while (f1 <= (len-1)) { #FORWARD begin time 
			#make data frame and add data for f1
			mc<-data.frame(time=rep(0,realizations),
					dist=rep(0,realizations))
			
			for (f2 in (f1+1):len) { #FORWARD end time	
				#angular distance based on both zen and azi 
				dist<-xs.dist.haversine(loc$zen[f1,],loc$azi[f1,],
						loc$zen[f2,],loc$azi[f2,])				
				
				#add to original data frame
				ndays<-days[f2]-days[f1]
				mc<-rbind(mc,data.frame(time=rep(ndays,realizations),
								dist=dist))
				
				#fit model
				mod<-xs.rate.linear(mc)
				#scale with sqrt(realization) to make noise 
				#independent of N realizations
				mod[2]<-mod[2]*sqrt(realizations)
				
				#FORWARD SNR: quality criterion
				#multiply with number of days to obtain signal 
				#of total movement during period
				if ((mod[1]*ndays/mod[2]) >= snr) {
					back.better<-FALSE #is a better solution backwards possible?
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting bakwards, starting with b1
						#make data frame and add data for b1
						mc<-data.frame(time=rep(0,realizations),
								dist=rep(0,realizations))
						
						for (b2 in (b1-1):(f1+1)) {
							#angular distance based on both zen and azi 
							dist<-xs.dist.haversine(loc$zen[b1,],loc$azi[b1,],
									loc$zen[b2,],loc$azi[b2,])				
							
							#add to original data frame
							back.ndays<-days[b1]-days[b2]
							mc<-rbind(mc,data.frame(time=rep(back.ndays,realizations),
											dist=dist))
							
							#fit model
							back.mod<-xs.rate.linear(mc)
							# 1) scale with sqrt(realization) to make noise 
							#    independent of N realizations
							back.mod[2]<-back.mod[2]*sqrt(realizations)
							
							#BACKWARD SNR: quality criterion
							# 2) multiply with number of days to obtain signal 
							#    of total movement during period
							if  ((back.mod[1]*back.ndays/back.mod[2]) >= snr) {
								back.better<-TRUE
								break #break backward width loop
							}#end if backward quality
						}#end b2 loop 
					}#=====END BACKWARD LOOP=================================
					#assign backward loop result
					if (back.better == TRUE) { #use backward
						now<-data.frame(time_beg=sub$generation_time[b2],
								time_end=sub$generation_time[b1],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   back.mod[1], #per day
								rate.sd=back.mod[2]) #per day	
						
						print("BACKWARD!!")
					}
					#assign forward loop results	
					if (back.better == FALSE) { #use forward
						now<-data.frame(time_beg=sub$generation_time[f1],
								time_end=sub$generation_time[f2],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   mod[1], #per day
								rate.sd=mod[2]) #per day	
						print("FORWARD!!")
					}
					
					rate<-rbind(rate,now)
					break #break t2 loop
				}#if SNR forward OK
				
			}#f2 for loop
			f1<-f2
		}#f1 while loop
	}#episodes for loop
	
	#return result
	return(rate)
} #----------------------------------------------------------------------------

xs.inc.rate.xy<-function(inc, dev.change, snr=3, width=3600) {
	#make sure data exists
	if (is.null(inc)==TRUE) return(NULL)	
	#ensure only one position is processed
	if (length(unique(inc$position))>1) return(NA)
	#sort by time
	inc<-inc[with(inc, order(generation_time)),] 
	
	#build device change episodes
	dc<-cumsum(diff(inc$device_id)!=0) #last time of one device_id is non-zero
	inc$epi<-c(0,dc[1:(nrow(inc)-1)])  #shift by one
	
	#new columns
	inc$rate   <-NA
	inc$rate.sd<-NA
	
	#make time stepping (parameter "width" [s])
	inc$days<-as.numeric(inc$generation_time)/86400
	step<-cumsum(diff(floor(as.numeric(inc$generation_time)/width))!=0 )
	inc$step<-c(0,step[1:(nrow(inc)-1)])+1 #consecutive steps
	
	#loop over device episodes
	for (e in unique(inc$epi)) {
		#subset for this episode
		sub<-subset(inc,epi==e)
		
		f1<-min(sub$step) #start with first value
		while (f1 <= (max(sub$step)-1)) { #FORWARD begin time 
			#index for f1
			if1<-inc$step==f1
			#make data frame and add data for f1
			mc<-data.frame(time=sub$days[if1],
					X=sub$x.DN[if1],
					Y=sub$y.DN[if1])
			for (f2 in (f1+1):max(inc$step)) { #FORWARD end time	
				#index for f2
				if2<-sub$step==f2
				#make data frame and add data for f2
				mc<-rbind(mc,data.frame(time=sub$days[if2],
								X=sub$x.DN[if2],Y=sub$y.DN[if2]))
				
				#fit model
				mod<-xs.rate.inc(mc)
				
				#FORWARD SNR: quality criterion
				if ((mod$rate/mod$rate.sd) >= snr) {
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting backwards, starting with b1
						#index for b1
						ib1<-sub$step==b1
						#make data frame and add data for b1
						mc<-data.frame(time=sub$days[ib1],
								X=sub$x.DN[ib1],
								Y=sub$y.DN[ib1])
						
						for (b2 in (b1-1):f1) {
							#index for f2
							ib2<-sub$step==b2
							#make data frame and add data for b2
							mc<-rbind(mc,data.frame(time=sub$days[ib2],
											X=sub$x.DN[ib2],Y=sub$y.DN[ib2]))
							
							#fit model
							back.mod<-xs.rate.inc(mc)
							
							#BACKWARD SNR: quality criterion
							if  ((back.mod$rate/back.mod$rate.sd) >= snr) break
						}#end b2 loop 
						
						#assign backward loop result
						ind<-inc$step>=b2 & inc$step<=b1
						inc$rate[ind]   <-back.mod$rate
						inc$rate.sd[ind]<-back.mod$rate.sd
						
						#new fit for remaining days if backward was shorter than foreward	
						if (b2 > f1) {
							ind<-sub$step>=f1 & sub$step<=b2
							mc<-data.frame(time=sub$days[ind],
									X=sub$x.DN[ind],
									Y=sub$y.DN[ind])
							#fit model
							res.mod<-xs.rate.inc(mc)
							ind<-inc$step>=f1 & inc$step<=b2
							inc$rate[ind]   <-res.mod$rate
							inc$rate.sd[ind]<-res.mod$rate.sd
						}
					} else {#=====END BACKWARD LOOP===================================
						#assign forward loop result, not enough data for backward
						ind<-inc$step>=f1 & inc$step<=f2
						inc$rate[ind]   <-mod$rate
						inc$rate.sd[ind]<-mod$rate.sd
					} #=====END BACKWARD ELSE=========================================
					break #break f2 loop to go to next f1
				}#if SNR forward OK
			}#f2 for loop
			print(c(f1,f2,round((mod$rate/mod$rate.sd)*10,0)))
			f1<-f2
		}#f1 while loop
	}#episodes for loop
	
	#return result
	return(inc)
} #-


#==============================================================================
# TEST FUNCTION FOR RATE OF ROTATION
#
# 
#
# Author: Stephan Gruber
#==============================================================================
xs.inc.rate.new.test<-function(plot.path,snr=2.5,sd.x=c(0.1, 0.1, 0.1),
		vel.x=c(0.1, 0.1, 0.1),realizations=1000,
		type="ORD") {
	#make mydev
	mydev<-data.frame(Label="Test",Sensor.Type="Synthetic",Position=9999)
	
	#make three episodes of data: fast,slow,fast
	dev.change<-data.frame(position=c(9999,9999,9999),
			device_id=c(1001,1002,1003),
			dbeg=c(ISOdatetime(1970,1, 1,12,0,0,tz="UTC"),
					ISOdatetime(1970,2, 1,12,0,0,tz="UTC"),
					ISOdatetime(1970,2,21,12,0,0,tz="UTC")),   
			dend=c(ISOdatetime(1970,1,31,12,0,0,tz="UTC"),
					ISOdatetime(1970,2,20,12,0,0,tz="UTC"),
					ISOdatetime(1970,2,28,12,0,0,tz="UTC")),
			duration=c(31,20,9))
	
	#make inclination data frame
	inc.data<-data.frame(generation_time=c(
					ISOdatetime(1970,1, 1,12,0,0,tz="UTC")+(0:30)*86400,
					ISOdatetime(1970,2, 1,12,0,0,tz="UTC")+(0:19)*86400,
					ISOdatetime(1970,2,28,12,0,0,tz="UTC")+(0:8) *86400),
			position  = c(rep(9999,31),rep(9999,20),rep(9999,9)),
			device_id = c(rep(1001,31),rep(1002,20),rep(1003,9)),
			mast.o    = rep(90,31+20+9),
			x.deg.med = c((1:31)*vel.x[1],31*vel.x[1]+(1:20)*
							vel.x[2],31*vel.x[1]+20*vel.x[2]+(1:9)*vel.x[3]),
			y.deg.med = rep(0,31+20+9),
			x.deg.sd  = c(rep(sd.x[1],31),rep(sd.x[2],20),rep(sd.x[3],9)),
			y.deg.sd  = rep(0,31+20+9))
	
	
	if (type=="SINE") {
		#convert
		DTOR<-pi/180
		#make inclination data frame
		inc.data<-data.frame(generation_time=c(
						ISOdatetime(1970,1, 1,12,0,0,tz="UTC")+(0:30)*86400,
						ISOdatetime(1970,2, 1,12,0,0,tz="UTC")+(0:19)*86400,
						ISOdatetime(1970,2,28,12,0,0,tz="UTC")+(0:8) *86400),
				position  = c(rep(9999,31),rep(9999,20),rep(9999,9)),
				device_id = c(rep(1001,31),rep(1002,20),rep(1003,9)),
				mast.o    = rep(90,31+20+9),
				x.deg.med = cumsum((sin((1:(31+20+9))*DTOR*8)+1)),
				y.deg.med = rep(0,31+20+9),
				x.deg.sd  = c(rep(sd.x[1],31),rep(sd.x[2],20),rep(sd.x[3],9)),
				y.deg.sd  = rep(0,31+20+9))
		
	}		
	
	inc.rate  <- xs.inc.rate(inc.data,dev.change,snr=snr,realizations=realizations)		
	
	print(inc.rate)
	
	#make plot
	plot_beg<-min(dev.change$dbeg)
	plot_end<-max(dev.change$dend)
	xs.plot.rate(inc.rate,dev.change,paste(plot.path,
					"plot_inc_pos_TEST.pdf",sep=""), mydev, type="INC",
			time_beg=plot_beg,time_end=plot_end)		
}#----------------------------------------------------------------------------





#==============================================================================
# RATE OF GPS ANTENNA MOVEMENT (TRANSLATION)
#
# Uses Monte-Carlo to propagate uncertainty and then calculates the rate of
# change based on a required signal-to-noise ratio. If this condition is not
# fulfilled, the time lag is increased until it is met. The rate of rotation
# is based on the great-circle distance (angle) between two points.
#
# INPUT inclinometer data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.o
#   - x.deg.med
#   - y.deg.med
#   - x.deg.sd
#   - y.deg.sd
#  ==> further parameters:
#   - snr           (signal-to-noise ratio)
#	- realizations	(number of Monte-Carlo realizations)
#
# RETURNS: new data frame with:
# 	- generation_time		
#	- position
#	- device_id
#	- rate    [deg/day]
#	- rate.sd [deg/day]
# ==> Plot with option type="s" in R to obtain steps.
#
# Author: Stephan Gruber
#==============================================================================
#---AUXILLIARY FUNCTION--------------------------------------------------------
xs.gps.rate.loc<-function(sub, realizations) {
	#get length
	len<-nrow(sub)
	
	#make matrices: N rows times R columns
	#add temporally-uncorrelated random noise
	X<-matrix(rep(sub$E.m,   realizations)+
					rep(sub$sdE.m, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol= realizations,
			byrow=FALSE)
	Y<-matrix(rep(sub$N.m,   realizations)+
					rep(sub$sdN.m, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol= realizations,
			byrow=FALSE)
	
	#return results
	return(list(X=X,Y=Y))
}
#---REAL FUNCTION--------------------------------------------------------------
xs.gps.rate<-function(gps, dev.change, snr=5,realizations=5000) {
	#make sure data exists
	if (is.null(gps)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	
	#ensure only one position is processed
	if (length(unique(gps$position))>1) return(NA)
	
	#loop over device episodes
	rate<-NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(gps,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)
		
		#compute matrix of R realizations for N point locations
		loc <-xs.gps.rate.loc(sub, realizations)
		days<-as.numeric(sub$generation_time)/86400
		
		f1<-1 #start with first value
		while (f1 <= (len-1)) { #FORWARD begin time 
			#make data frame and add data for f1
			mc<-data.frame(time=rep(0,realizations),
					dist=rep(0,realizations))
			
			for (f2 in (f1+1):len) { #FORWARD end time					
				#cartesian distance
				dist<-xs.dist.xyz(data.frame(X=loc$X[f1,],Y=loc$Y[f1,]),
						data.frame(X=loc$X[f2,],Y=loc$Y[f2,]),
						type="horizontal")
				
				#add to original data frame
				ndays<-days[f2]-days[f1]
				mc<-rbind(mc,data.frame(time=rep(ndays,realizations),
								dist=dist))
				
				#fit model
				mod<-xs.rate.linear(mc)
				#scale with sqrt(realization) to make noise 
				#independent of N realizations
				mod[2]<-mod[2]*sqrt(realizations)
				
				#FORWARD SNR: quality criterion
				#multiply with number of days to obtain signal 
				#of total movement during period
				if ((mod[1]*ndays/mod[2]) >= snr) {
					back.better<-FALSE #is a better solution backwards possible?
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting bakwards, starting with b1
						#make data frame and add data for b1
						mc<-data.frame(time=rep(0,realizations),
								dist=rep(0,realizations))
						
						for (b2 in (b1-1):(f1+1)) {			
							#cartesian distance
							dist<-xs.dist.xyz(data.frame(X=loc$X[b1,],Y=loc$Y[b1,]),
									data.frame(X=loc$X[b2,],Y=loc$Y[b2,]),
									type="horizontal")
							
							#add to original data frame
							back.ndays<-days[b1]-days[b2]
							mc<-rbind(mc,data.frame(time=rep(back.ndays,realizations),
											dist=dist))
							
							#fit model
							back.mod<-xs.rate.linear(mc)
							# 1) scale with sqrt(realization) to make noise 
							#    independent of N realizations
							back.mod[2]<-back.mod[2]*sqrt(realizations)
							
							#BACKWARD SNR: quality criterion
							# 2) multiply with number of days to obtain signal 
							#    of total movement during period
							if  ((back.mod[1]*back.ndays/back.mod[2]) >= snr) {
								back.better<-TRUE
								break #break backward width loop
							}#end if backward quality
						}#end b2 loop 
					}#=====END BACKWARD LOOP=================================
					#assign backward loop result
					if (back.better == TRUE) { #use backward
						now<-data.frame(time_beg=sub$generation_time[b2],
								time_end=sub$generation_time[b1],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   back.mod[1], #per day
								rate.sd=back.mod[2]) #per day	
						
						print("BACKWARD!!")
					}
					#assign forward loop results	
					if (back.better == FALSE) { #use forward
						now<-data.frame(time_beg=sub$generation_time[f1],
								time_end=sub$generation_time[f2],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   mod[1], #per day
								rate.sd=mod[2]) #per day	
						print("FORWARD!!")
					}
					
					rate<-rbind(rate,now)
					break #break t2 loop
				}#if SNR forward OK
				
			}#f2 for loop
			f1<-f2
		}#f1 while loop
	}#episodes for loop
	
	#return result
	return(rate)
} #----------------------------------------------------------------------------



#==============================================================================
# RATE OF GPS FOOT MOVEMENT (TRANSLATION CORRECTED FOR ROTATION OF MAST)
#
# Uses Monte-Carlo to propagate uncertainty and then calculates the rate of
# change based on a required signal-to-noise ratio. If this condition is not
# fulfilled, the time lag is increased until it is met. The rate of rotation
# is based on the great-circle distance (angle) between two points.
#
# INPUT inclinometer data frame with:
# 	- generation_time
#	- position
#	- device_id
#   - mast.o
#   - x.deg.med
#   - y.deg.med
#   - x.deg.sd
#   - y.deg.sd
#  ==> further parameters:
#   - snr           (signal-to-noise ratio)
#	- realizations	(number of Monte-Carlo realizations)
#
# RETURNS: new data frame with:
# 	- generation_time		
#	- position
#	- device_id
#	- rot.rate    [deg/day]
#	- rot.rate.sd [deg/day]
# ==> Plot with option type="s" in R to obtain steps.
#
# Author: Stephan Gruber
#==============================================================================
#---AUXILLIARY FUNCTION--------------------------------------------------------
xs.com.rate.loc<-function(sub, realizations) {
	#get length
	len<-nrow(sub)
	
	#make matrices: N rows times R columns
	#add temporally-uncorrelated random noise
	x.deg<-matrix(rep(sub$x.deg.med,realizations)+
					rep(sub$x.deg.sd, realizations)*
					rnorm(len*        realizations),
			nrow=len,ncol=    realizations,
			byrow=FALSE)
	y.deg<-matrix(rep(sub$y.deg.med,realizations)+
					rep(sub$y.deg.sd, realizations)*
					rnorm(len*        realizations),
			nrow=len,ncol=    realizations,
			byrow=FALSE)
	angle<-matrix(rep(sub$mast.o,realizations),
			nrow=len,ncol= realizations,
			byrow=FALSE)
	
	#calculate inc/azi in global system
	loc<-xs.inc.loc2glob(x.deg,y.deg,angle=angle)
	
	#make a matrix of it
	azi<-matrix(loc$azi.deg,nrow=len,ncol=realizations,byrow=FALSE)
	zen<-matrix(loc$zen.deg,nrow=len,ncol=realizations,byrow=FALSE)
	
	
	
	
	#make matrices: N rows times R columns
	#add temporally-uncorrelated random noise
	X<-matrix(rep(sub$E.m,   realizations)+
					rep(sub$sdE.m, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol= realizations,
			byrow=FALSE)
	Y<-matrix(rep(sub$N.m,   realizations)+
					rep(sub$sdN.m, realizations)*
					rnorm(len*realizations),
			nrow=len,ncol= realizations,
			byrow=FALSE)
	
	
	
	#get position 1, correct for mast tilt, add temporally-uncorrelated random noise 
	t1xy<-xs.com.pre(rep(sub$E.m[t1],    realizations)+ #E.m
					rep(sub$sdE.m[t1],  realizations)*rnorm(realizations),
			rep(sub$N.m[t1],    realizations)+ #N.m
					rep(sub$sdN.m[t1],  realizations)*rnorm(realizations),
			rep(sub$h.m[t1],    realizations)+ #h.m
					rep(sub$sdh.m[t1],  realizations)*rnorm(realizations),
			azi, zen, #matrixes of zen and azi
			rep(sub$mast.h[t1], realizations)+ #mast.h
					mast.h.sd*rnorm(realizations))			
	
	
	#return results
	return(list(X=X,Y=Y))
}
#---REAL FUNCTION--------------------------------------------------------------
xs.com.rate<-function(com, dev.change, snr=5,realizations=5000) {
	#make sure data exists
	if (is.null(com)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	
	#ensure only one position is processed
	if (length(unique(com$position))>1) return(NA)
	
	#loop over device episodes
	rate<-NULL
	for (epi in 1:length(dev.change$position)) {
		#subset for this episode
		sub<-subset(com,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)
		
		#compute matrix of R realizations for N point locations
		loc <-xs.com.rate.loc(sub, realizations)
		days<-as.numeric(sub$generation_time)/86400
		
		f1<-1 #start with first value
		while (f1 <= (len-1)) { #FORWARD begin time 
			#make data frame and add data for f1
			mc<-data.frame(time=rep(0,realizations),
					dist=rep(0,realizations))
			
			for (f2 in (f1+1):len) { #FORWARD end time					
				#cartesian distance
				dist<-xs.dist.xyz(data.frame(X=loc$X[f1,],Y=loc$Y[f1,]),
						data.frame(X=loc$X[f2,],Y=loc$Y[f2,]),
						type="horizontal")
				
				#add to original data frame
				ndays<-days[f2]-days[f1]
				mc<-rbind(mc,data.frame(time=rep(ndays,realizations),
								dist=dist))
				
				#fit model
				mod<-xs.rate.linear(mc)
				#scale with sqrt(realization) to make noise 
				#independent of N realizations
				mod[2]<-mod[2]*sqrt(realizations)
				
				#FORWARD SNR: quality criterion
				#multiply with number of days to obtain signal 
				#of total movement during period
				if ((mod[1]*ndays/mod[2]) >= snr) {
					back.better<-FALSE #is a better solution backwards possible?
					back.len<-1+f2-f1  #total amount of time to work with
					#=====BACKWARD LOOP========================================
					if (back.len>=3) { #only usful for more than two lags
						b1<-f2 #counting bakwards, starting with b1
						#make data frame and add data for b1
						mc<-data.frame(time=rep(0,realizations),
								dist=rep(0,realizations))
						
						for (b2 in (b1-1):(f1+1)) {			
							#cartesian distance
							dist<-xs.dist.xyz(data.frame(X=loc$X[b1,],Y=loc$Y[b1,]),
									data.frame(X=loc$X[b2,],Y=loc$Y[b2,]),
									type="horizontal")
							
							#add to original data frame
							back.ndays<-days[b1]-days[b2]
							mc<-rbind(mc,data.frame(time=rep(back.ndays,realizations),
											dist=dist))
							
							#fit model
							back.mod<-xs.rate.linear(mc)
							# 1) scale with sqrt(realization) to make noise 
							#    independent of N realizations
							back.mod[2]<-back.mod[2]*sqrt(realizations)
							
							#BACKWARD SNR: quality criterion
							# 2) multiply with number of days to obtain signal 
							#    of total movement during period
							if  ((back.mod[1]*back.ndays/back.mod[2]) >= snr) {
								back.better<-TRUE
								break #break backward width loop
							}#end if backward quality
						}#end b2 loop 
					}#=====END BACKWARD LOOP=================================
					#assign backward loop result
					if (back.better == TRUE) { #use backward
						now<-data.frame(time_beg=sub$generation_time[b2],
								time_end=sub$generation_time[b1],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   back.mod[1], #per day
								rate.sd=back.mod[2]) #per day	
						
						print("BACKWARD!!")
					}
					#assign forward loop results	
					if (back.better == FALSE) { #use forward
						now<-data.frame(time_beg=sub$generation_time[f1],
								time_end=sub$generation_time[f2],
								position= dev.change$position[epi], 
								device_id=dev.change$device_id[epi],
								rate=   mod[1], #per day
								rate.sd=mod[2]) #per day	
						print("FORWARD!!")
					}
					
					rate<-rbind(rate,now)
					break #break t2 loop
				}#if SNR forward OK
				
			}#f2 for loop
			f1<-f2
		}#f1 while loop
	}#episodes for loop
	
	#return result
	return(rate)
} #----------------------------------------------------------------------------




xs.com.rate.old<-function(com, dev.change, snr=4/1,realizations=500,mast.h.sd=0.005) {
	#make sure data exists
	if (is.null(com)       ==TRUE) return(NULL)
	if (is.null(dev.change)==TRUE) return(NULL)
	#ensure only one position is processed
	if (length(unique(com$position))>1) return(NA)
	
	#loop over device episodes
	rate<-NULL
	for (epi in 1:length(dev.change$position)) {
		#take subset for this period
		sub<-subset(com,generation_time>=dev.change$dbeg[epi] & 
						generation_time<=dev.change$dend[epi] )
		
		#beg work-------------------------------------------
		sub<-sub[with(sub, order(generation_time)),] #sort by time
		len<-length(sub$position)
		t1<-1 #start with first value
		rate.yes<-FALSE
		while (t1 <= (len-1)) { #begin time step velocity
			#make data frame and add data for t1
			#memorize time (days) for later subtraction
			t1t<-as.numeric(sub$generation_time[t1])/86400
			mc<-data.frame(time=rep(0,realizations),
					dist=rep(0,realizations))
			
			#add temporally-uncorrelated random noise, get position 1 
			inc1<-xs.inc.loc2glob(rep(sub$x.deg.med[t1],realizations)+
							rep(sub$x.deg.sd[t1], realizations)*
							rnorm(realizations),
					rep(sub$y.deg.med[t1],realizations)+
							rep(sub$y.deg.sd[t1], realizations)*
							rnorm(realizations),
					angle=sub$mast.o[t1])
			
			#get position 1, correct for mast tilt, add temporally-uncorrelated random noise 
			t1xy<-xs.com.pre(rep(sub$E.m[t1],    realizations)+ #E.m
							rep(sub$sdE.m[t1],  realizations)*rnorm(realizations),
					rep(sub$N.m[t1],    realizations)+ #N.m
							rep(sub$sdN.m[t1],  realizations)*rnorm(realizations),
					rep(sub$h.m[t1],    realizations)+ #h.m
							rep(sub$sdh.m[t1],  realizations)*rnorm(realizations),
					inc1$azi.deg, #azi.deg
					inc1$zen.deg, #inc.deg
					rep(sub$mast.h[t1], realizations)+ #mast.h
							mast.h.sd*rnorm(realizations))	
			
			for (t2 in (t1+1):len) { #end time step velocity	
				#time
				time<-rep(as.numeric(sub$generation_time[t2])/86400-t1t,realizations)
				
				#add temporally-uncorrelated random noise, get position 2
				inc2<-xs.inc.loc2glob(rep(sub$x.deg.med[t2],realizations)+
								rep(sub$x.deg.sd[t2], realizations)*
								rnorm(realizations),
						rep(sub$y.deg.med[t2],realizations)+
								rep(sub$y.deg.sd[t2], realizations)*
								rnorm(realizations),
						angle=sub$mast.o[t2])
				
				#get position 2, correct for mast tilt, add temporally-uncorrelated random noise
				t2xy<-xs.com.pre(rep(sub$E.m[t2],    realizations)+ #E.m
								rep(sub$sdE.m[t2],  realizations)*rnorm(realizations),
						rep(sub$N.m[t2],    realizations)+ #N.m
								rep(sub$sdN.m[t2],  realizations)*rnorm(realizations),
						rep(sub$h.m[t2],    realizations)+ #h.m
								rep(sub$sdh.m[t2],  realizations)*rnorm(realizations),
						inc2$azi.deg, #azi.deg
						inc2$zen.deg, #inc.deg
						rep(sub$mast.h[t2], realizations)+ #mast.h
								mast.h.sd*rnorm(realizations))		
				
				#cartesian distance
				dist<-xs.dist.xyz(t1xy,t2xy,type="horizontal")
				
				#add to original data frame
				mc.now<-data.frame(time=time,dist=dist)
				names(mc.now)<-c("time","dist")
				mc<-rbind(mc,mc.now)
				
				#fit model
				mod<-xs.rate.linear(mc)	
				r<-mod[1]
				sd.r<-mod[2]
				
				#decide based on SNR
				if ((r/sd.r) > snr) {
					rate.yes<-TRUE
					now<-data.frame(generation_time=sub$generation_time[t1], 
							position= dev.change$position[epi], 
							device_id=dev.change$device_id[epi])
					now$rate   <-   r #per day
					now$rate.sd<-sd.r #per day
					rate<-rbind(rate,now)
					t1<-t2-1 #to avoid double increment later		
					break
				}
			}#t2
			t1<-t1+1
		}#t1
		#add last time step
		if (rate.yes==TRUE) {
			now<-data.frame(generation_time=sub$generation_time[t1], 
					position= dev.change$position[epi], 
					device_id=dev.change$device_id[epi])
			now$rate   <-   r #per day
			now$rate.sd<-sd.r #per day
			rate<-rbind(rate,now)
		}
		#end work------------------------------------------
	}#episode
	
	#return result
	return(rate)
} #----------------------------------------------------------------------------





#==============================================================================
# MAKE NICE PLOT
#
# Detects device changes.
#
# Only process one position at a time!!
#
# INPUT daily inclinometer data frame with:
# 	- generation_time
#	- position
#	- device_id
#
# RETURNS: data.frame with:
#	- position
#	- device_id
#	- dbeg   
#	- dend
#	- duration
#
# Author: Stephan Gruber
#==============================================================================

#-- convert rate format that has begin/end for stair plotting -------
xs.rate.stairs<-function(rate,dev.change) {
	#it does not hurt to have begin and end of every step as a
	#separate time
	
	#old rate calculation
	if (is.null(rate$generation_time)==FALSE) return(rate)
	
	#loop over lines
	data<-NULL
	for (l in 1:(nrow(rate)-1)) {
		data<-rbind(data, #only start date and value
				data.frame(generation_time=rate$time_beg[l],
						position       =rate$position[l],
						device_id      =rate$device_id[l],
						rate           =rate$v[l],
						rate.sd        =rate$v.sd[l]))
		#if end and next start do not match, insert NA
		if (rate$time_end[l]!=rate$time_beg[l+1]) {
			data<-rbind(data, #only start date and value
					data.frame(generation_time=rate$time_end[l],
							position       =rate$position[l],
							device_id      =rate$device_id[l],
							rate           =rate$v[l],
							rate.sd        =rate$v.sd[l]),
					data.frame(generation_time=rate$time_end[l],
							position       =rate$position[l],
							device_id      =rate$device_id[l],
							rate           =NA,
							rate.sd        =NA))
		}
	}
	return(data)
}

#PLOT rate of inclination change ------------------------------------------
xs.plot.rate<-function(rate, dev.change, pdf.file, mydev, type="inc", width=10, height=5,
		time_beg="2010-12-01", time_end="2013-01-01") {
	#if no data is there to plot, then exit
	valid<-TRUE
	if (is.null(rate)==TRUE) valid<-FALSE
	if (length(rate$position) <= 0) valid<-FALSE
	if (valid == FALSE) {
		print('No data to plot.')
		return(NULL)
	}
	
	#make good label
	site<-" ("&mydev$Label[1]&" | "&mydev$Sensor.Type[1]&")"
	
	#distinguish plot types
	if (toupper(type == "INC"))  { #RATE OF TILTING
		main<-"Rate of rotation for position "&mydev$Position[1]&site
		ylab<-"Rate of rotation [deg/day]"
		col<-"red"
	}
	if (toupper(type == "GPS 2D"))  { #VELOCITY GPS ANTENNA
		main<-"Velocity (at antenna) for position "&mydev$Position[1]&site
		ylab<-"Velocity [m/day]"		
		col<-"blue"
	}
	if (toupper(type == "FOOT")) { #VELOCITY MAST FOOT
		main<-"Velocity (at mast foot) for position "&mydev$Position[1]&site
		ylab<-"Velocity [m/day]"	
		col<-"green"
	}
	
	#adjust width
	CTI<-0.393700787 #cm to inch by multiplication
	width <-width*CTI
	height<-height*CTI
	
	#convert times, make xlim
	time_beg<-as.POSIXct(time_beg)
	time_end<-as.POSIXct(time_end)
	xlim<-c(time_beg,time_end)
	
	#make ylim
	ymax<-max(rate$rate+rate$rate.sd)
	ylim=c(0,ymax*1.1)
	
	#make date-label
	datelabel<- seq(from=time_beg, to=time_end, by="2 month")
	
	#make plot
	
	#output to PDF
	pdf(file=pdf.file,width,height)
	
	#make rate plottable
	data<-xs.rate.stairs(rate,dev.change)		
	
	#+/- one sd
	plot(data$generation_time,data$rate+data$rate.sd,type="s",col="azure3",
			xlim=xlim,ylim=ylim,xlab="date",ylab=ylab,main=main, axes=F)
	lines(data$generation_time,data$rate-data$rate.sd,type="s",col="azure3")	
	
	#make axis	
	axis(2, las=2) 
	axis(1, at=datelabel, tick= T, labels= strftime(datelabel,"%Y/%m"), xlim=xlim);box()
	
	#mean
	lines(data$generation_time,data$rate,type="s",col=2,lwd=1.0)
	
	#add arrows
	if (is.null(dev.change)==FALSE) xs.plot.arrows(dev.change,ymax)
	
	dev.off() #end PDF	
}#-----------------------------------------------------------------------------




#==============================================================================
# APPLY COORDINATE TRANSFORMATION FROM LOCAL TO GLOBAL (CH1903) COORDINATES 
#FOR ENTIRE DATA.FRAME
# converts values of data frame (inc.daily from function xs.inc.ready) to 
# global coordinates
#
#	
# INPUT data.frame with columns: 
#	- generation_time
#	- position
#	- device_id
#   - mast.o -> linear interpoalted between measurements (made with device-change)
#   - mast.h
#	- x.deg.lm (daily value based on linear regression)   
#	- y.deg.lm (daily value based on linear regression)
#	- x.deg.mu (mean per day)
#	- y.deg.mu (mean per day)
#	- x.deg.sd (standard deviation per day)  
#	- y.deg.sd (standard deviation per day)
#	- count (number of measurements per day)
#
#	RETURNS:
# -> same as input, but with zen (total inclination) and azi (direction/aspect
# 		of zen) of fitted values (lm) and sd per day
#
# Author: Vanessa Wirz
#==============================================================================
xs.inc.glob <- function(inc.daily){
	#convert to glbal coordinates
	lm<- xs.inc.loc2glob(x.deg=inc.daily$x.deg.lm,y.deg=inc.daily$y.deg.lm,
			angle=inc.daily$mast.o)
	std<- xs.inc.loc2glob(x.deg=inc.daily$x.deg_sd,y.deg=inc.daily$y.deg_sd,
			angle=0)
	
	# make data.frame
	inc.glob<- data.frame(generation_time=inc.daily$generation_time,
			position=inc.daily$position,
			mast.o=inc.daily$mast.o,
			mast.h=inc.daily$mast.h,
			zen.deg.lm=lm$zen.deg,
			azi.deg.lm=lm$azi.deg,
			zen.deg.sd=std$zen.deg,
			azi.deg.sd=std$azi.deg)
	
	#return data
	return(inc.glob)
}
