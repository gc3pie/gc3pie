GetWeight <- function(edge, data){
# ==============================================================================
# Computes the weight of an edge in the one mode network

# Args:
# - edge: edge from the one mode network. It is a vector of two elements:
#         the source node and the target node
# - data: a data frame with the edgelist for the two mode network

# Returned values/objects:
# - the weight

# Libraries required:
# ==============================================================================

#
    source.node <- as.character(edge[1])
    target <- as.character(edge[2])

# Get the names of the threads were both the source and the target posted
    threads.target.posted <- data[data$User==target,"UniqueThread"]
    threads.source.posted <-data[data$User==source.node, "UniqueThread"]
    shared.threads <- intersect(threads.target.posted,threads.source.posted)

# For each shared thread, get the index of the corresponding rows in the data
    index <- numeric()
    for(k in shared.threads)
        index <- c(index,which(data$UniqueThread==k))

# The numerator is the number of posts the target made in all the shared
# threads with source
    numerator <-sum(subset(data[index,], User==target, select=NrPosts))

# Get the names of the threads where the source posted
    threads.source.posted <- subset(data, User==source.node, select=UniqueThread)

# The denominator is the number of post all other nodes (except the source) made in
# the shared threads with the source
    denominator <- numeric()
    for(j in 1:nrow(threads.source.posted)){
        thread.posts <-  subset(data, UniqueThread==threads.source.posted[j,])
        index <- which(thread.posts$User!= source.node)
        denominator[j] <- sum(thread.posts[index, "NrPosts"])

    }

# Return the weight
    numerator/sum(denominator)
}

