##############################GRID COMPUTATION TEST#####################################
library('igraph')
grid_test <- function(id_time_sample, friendship_nw_sample) {
  library(igraph)
  frd_nw_betweenness <- data.frame(id=character(0), id_check=character(0), betweenness=numeric(0), stringsAsFactors=F)
  j <- 1
  for (i in id_time_sample$id) {
    subset.i <- subset(friendship_nw_sample, friendship_nw_sample$t_friendcreated <= subset(id_time_sample, id_time_sample$id == i)$tf_reference)
    graph.i <- graph.data.frame(subset.i)
    df.i <- data.frame(id=V(graph.i)$name, betweenness=betweenness(graph.i,directed=F, weights=NULL), stringsAsFactors=F)
    nw_betweenness.i <- subset(df.i, df.i$id == i)
    frd_nw_betweenness[j,] <- c(i, nw_betweenness.i[1,])
    j <- j+1
  }
frd_nw_betweenness
}

###########################EXAMPLE##########################
# test <- grid_test(id_time_sample=id_time_sample[1:10, ], friendship_nw_sample=friendship_nw_sample)


