args <- commandArgs(trailingOnly = TRUE)

library('multicore')

# source('f_get_weight.r')
# load('two_mode_network.rda')
# load('edgelist_one_mode.rda')


# Sergio?
# yes
# can we run an R shell on an 2nd screen term and load the data?
# sure, but why don't we use the skype chat for chat and the screen for commands ?
# ok let do it

if(length(args)!=3){
  print("Usage: R CMD BATCH '--args <R_evaluation_function.R> <edges_file.rda> <data_network.rda>' run.R <std_out_file>")
}else{
  # read function
  source(args[1])

  # Read edges file
  # loading the dataset could be slow
  time_to_load_edges <- system.time(input.edges<-get(load(file=args[2])))

  # Read network data file
  time_to_load_data <- system.time(input.data<-get(load(file=args[3])))
}

cat("Time to load input data: edges [",time_to_load_edges,"], data [",time_to_load_data,"]\n")

# loop over the edges and call the loaded function
# trust the function will be called 'GetWeight'

# weight_list<-apply(input.edges,1,GetWeight,data=input.data)
# this is just lapply for multicore
# Yes, but I did not managed to test it yet
# the one working so far is the 'apply' commented out
# no it is the replacement for lapply. lapply is not apply!!!
# True but I guess you're referring to mclapply
# I read mcapply is a replacement for sapply
# anyhow, at the moment I'm interested in 'apply'
# yep. let go ahead
# weigth_list <- mcapply(input.edges, 1, GetWeight,data=input.data, mc.preschedule = T, mc.set.seed = F, mc.cores = 2)

input.edges <- as.list(as.data.frame(t(input.edges[,c(1,2)])))
cat("Input edges: ")
print(input.edges)

system.time(weight_list <- mclapply(input.edges, GetWeight, data=input.data))

# here i would use an *apply function later for performace ....
# for(i in 1:nrow(input.edges)) {
#    input.edges[i,3] = weight_list[i]
# }

# Suggestion from C. Panse
input.edges <- cbind(input.edges,weight_list)

print("Modified edges")
print(input.edges)

# store result
# save(input.edges,file="edges.weight.rda")
write.csv(input.edges, "result.csv", sep='\t')

print("Done")

