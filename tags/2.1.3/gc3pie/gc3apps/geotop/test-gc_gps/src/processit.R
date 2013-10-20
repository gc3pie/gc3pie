args=(commandArgs(TRUE)) ##read in the arguments listed at the command line

##args is now a list of character vectors
## First check to see if arguments are passed.
## Then cycle through each element of the list and evaluate the expressions.
if(length(args)==0){
   print("No arguments supplied.")
}else{
   for(i in 1:length(args)){
     eval(parse(text=args[[i]]))
   }
}

#set specific stuff
setwd("./") #path
source("./src/gps.utils.R") #source utils to have functionality 
data.wd="./out/" #path to store output-data

#execute command
xs.process.position(pos=pos, realizations=realizations, snr=snr, mast.h=mast.h, sd.mast.o=sd.mast.o)
