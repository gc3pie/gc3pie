% set reference point in time
tic

% seed RNG with current time so we un-predictable random numbers,
% i.e. each time we run the script N will be different
rng('shuffle') 

% random array size
N = fix(rand(1)*1e9) 

% try to allocate array of size N -- must suppress output else MATLAB
% tries to print out the array in full...
a = NaN(N,1); 

% print time elapsed since `tic`
toc 
