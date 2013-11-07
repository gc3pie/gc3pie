# Expect as arguments:
# . R function to apply
# . id_time_sample.csv
# . friendship_nw_sample.csv
args <- commandArgs(trailingOnly = TRUE)

if(length(args)!=3){
  print("Usage: R CMD BATCH '--args <R_evaluation_function.R> <id_time.csv> <friendship_network.csv>' run.R <std_out_file>")
}else{
  # read the R function
  # XXX: how to check whether the sources file indeed contains the expected function ?
  source(args[1])

  # Read ID time file
  time_to_load_id_time <- system.time(input.id_time <-read.csv(file=args[2],head=TRUE, sep=","))
  # Read friendship network file
  time_to_load_friendship <- system.time(input.friendship_nw <-read.csv(file=args[3],head=TRUE, sep=","))
}

cat("Time to load input data: ID time [",time_to_load_id_time,"], data [",time_to_load_friendship,"]\n")

# loop over the edges and call the loaded function
# XXX: trust the function will be called 'grid_test'
time_to_process <- system.time(frd_nw_betweenness <- grid_test(id_time_sample=input.id_time[1:10, ], friendship_nw_sample=input.friendship_nw))

cat("Time to process betweenness [",time_to_process,"]\n")

# store result
write.table(frd_nw_betweenness, "result.csv", quote=TRUE, row.names=FALSE, col.names=TRUE, sep=",")

print("Done")

