#!/usr/bin/Rscript

## -----------------------------------------------------------------------------
# Generate sample paths for assets assuming geometric Brownian motion.
#
# Inputs: S0 - stock price today (e.g., 50)
#       : mu - expected return (e.g., 0.04)
#       : sig - volatility (e.g., 0.1)
#       : dt - size of time steps (e.g., 0.273)
#       : etime - days to expiry (e.g., 1000)
#       : nsims - number of simulation paths to generate
#
# Output:
#   - "results.csv" dump of a matrix where each column
#     represents a simulated asset price path.
#   - "results.pdf" line plot of all simulated price path
#
# Notes: This code focuses on details of the implementation of the
#        Monte-Carlo algorithm.
#        It does not contain any programatic essentials such as error
#        checking.
#        It does not allow for optional/default input arguments.
#        It is not optimized for memory efficiency nor speed.
#
# Original MATLAB code by Phil Goddard (phil@goddardconsulting.ca), Date: Q2, 2006
# Adapted to R by Riccardo Murri <riccardo.murri@gmail.com>, 2016-11-16
#
## -----------------------------------------------------------------------------


assetPaths <- function(S0, mu, sig, dt, steps, nsims) {

  ## calculate the drift
  nu <- mu - sig*sig/2;

  ## Generate potential paths:

  # all paths start at (relative) price 1
  top <- rep(1, nsims);  # vector of 1's

  # generate random matrix with `steps` rows and `nsims` columns
  # (`apply(matrix, 1, fn)` => apply `fn` to each row of matrix;
  # `apply(matrix, 2, fn)` => apply `fn` to each column of matrix)
  rnd <- t(apply(matrix(c(NA), steps, nsims), 1, function(x) { rnorm(nsims); }));

  # compute paths
  X <- exp(nu*dt + sig*sqrt(dt)*rnd);
  Y <- apply(X, 2, cumprod);  # column-wise cumulative product
  relpaths <- rbind(top, Y);

  # multiply by initial price S0 to get the final result
  S0 * relpaths;
}


plotPaths <- function(S, filename) {
  pdf(filename, 13, 10);
  
  data <- t(S)
  nsims <- dim(data)[1]
  nsteps <- dim(data)[2]

  # get the range for the x and y axis
  xrange <- c(1, nsteps)
  yrange <- range(data)

  # set up the plot
  plot(xrange, yrange, type="n",
     xlab="Time to expiry (days)",
     ylab="Asset Value (US$)" )
  colors <- rainbow(nsims)
  linetype <- c(1:nsims)
  plotchar <- seq(18,18+nsims,1)

  # add lines
  for (i in 1:nsims) {
    lines(1:nsteps, data[i,], type="b", lwd=1.5,
      lty=linetype[i], col=colors[i], pch=plotchar[i])
  }

  # add a title and subtitle
  title("Asset Pricing", "simulated pricing")

  # add a legend
  legend(xrange[1], yrange[2], 1:nsims, cex=0.8, col=colors,
    pch=plotchar, lty=linetype, title="Simulation path");
}


## -----------------------------------------------------------------------------
## Commands to run simulation
## -----------------------------------------------------------------------------

## get arguments from command line
args <- commandArgs(TRUE)

S0  <- as.numeric(args[1]);    # Price of underlying today
mu  <- as.numeric(args[2]);    # expected return
sig <- as.numeric(args[3]);    # expected volatility
dt  <- as.numeric(args[4]);    # time steps
etime <- as.numeric(args[5]);  # days to expiry
nsims <- as.numeric(args[6]);  # Number of simulated paths

paste("DEBUG: S0=", S0);
paste("DEBUG: mu=", mu);
paste("DEBUG: sig=", sig);
paste("DEBUG: dt=", dt);
paste("DEBUG: etime=", etime);
paste("DEBUG: nsims=", nsims);

## Generate potential future asset paths
S <- assetPaths(S0, mu, sig, dt, etime, nsims);

## write out results
write.table(t(S), "results.csv", row.names=FALSE, col.names=FALSE, sep=",");
plotPaths(S, "results.pdf");
