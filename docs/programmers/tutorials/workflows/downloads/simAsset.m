function simAsset(S0,mu,sig,dt,etime,nruns,result_prefix)

% Script to price an Asian put option using a Monte-Carlo approach.

  S0 = str2num(S0);
  mu = str2num(mu);
  sig = str2num(sig);
  dt = str2num(dt);
  etime = str2num(etime);
  nruns = str2num(nruns);

% S0 =50;       % Price of underlying today
  X = 155;       % Strike at expiry
% mu = 0.04;    % expected return
% sig = 0.1;    % expected vol.
  r = 0.03;     % Risk free rate
% dt = 10/365;   % time steps
% etime = 450;   % days to expiry
  T = dt*etime; % years to expiry

% nruns = 3000; % Number of simulated paths

% Generate potential future asset paths
  S = AssetPaths(S0,mu,sig,dt,etime,nruns);

% Plot the asset paths
  time = etime:-1:0;
  plot(time,S);
  set(gca,'XDir','Reverse','FontWeight','bold','Fontsize',10);
  xlabel('Time to Expiry','FontWeight','bold','Fontsize',10);
  ylabel('Asset Price','FontWeight','bold','Fontsize',10);
  title('Simulated Asset Paths','FontWeight','bold','Fontsize',10);
  grid on
  set(gcf,'Color','w');
  mkdir('results')
  print(strcat('results/image_',result_prefix),'-dpng');
end

function S = AssetPaths(S0,mu,sig,dt,steps,nsims)
% Function to generate sample paths for assets assuming geometric
% Brownian motion.
%
% S = AssetPaths(S0,mu,sig,dt,steps,nsims)
%
% Inputs: S0 - stock price
%       : mu - expected return
%       : sig - volatility
%       : dt - size of time steps
%       : steps - number of time steps to calculate
%       : nsims - number of simulation paths to generate
%
% Output: S - a matrix where each column represents a simulated
%             asset price path.
%
% Notes: This code focuses on details of the implementation of the
%        Monte-Carlo algorithm.
%        It does not contain any programatic essentials such as error
%        checking.
%        It does not allow for optional/default input arguments.
%        It is not optimized for memory efficiency or speed.

% Author: Phil Goddard (phil@goddardconsulting.ca)
% Date: Q2, 2006

% calculate the drift
  nu = mu - sig*sig/2;

% Generate potential paths
  S = S0*[ones(1,nsims); ...
          cumprod(exp(nu*dt+sig*sqrt(dt)*randn(steps,nsims)),1)];
end
