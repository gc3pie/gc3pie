library('data.table')
GetWeight <- function(edge, data, threads.nodes.posted){
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
    shared.threads <- as.character(intersect(threads.nodes.posted[[target]]$UniqueThread,threads.nodes.posted[[source.node]]$UniqueThread))


# The numerator is the number of posts the target made in all the shared
# threads with source
#    numerator <- sum(data[J(shared.threads, source.node), 3, with=FALSE])

numerator <- sum(data[J(shared.threads, source.node), 3, with=FALSE])


# Get the names of the threads where the source posted
    threads.source.posted <- threads.nodes.posted[[source.node]]$UniqueThread

# The denominator is the number of post all other nodes (except the source) made in
# the shared threads with the source
 
    thread.posts <- data[threads.source.posted]
    thread.posts <- thread.posts[-thread.posts[User==source.node, which=TRUE]]
    denominator <- sum(thread.posts$NrPosts)

# Return the weight
    numerator/sum(denominator)
}

