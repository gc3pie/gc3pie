function estimateDCM(param_file,data_file,result_path)
% estimate_DCM_from_param_new estimates DCM models using SCRalyze
%
%_____________________________________
% (C) 2013 Matthias Staib (Zurich University Hospital for Psychiatry)
%
% v002 ms 01.12.2013 mstaib, Using data from sound experiment now
% v003 ms 28.10.2014 new VBA version from 27.10.2014
% v004 ms 14.11.2014 introduced function, simplified execution (sergio.maffioletti@s3it.uzh.ch)


% S3IT: This should not be necessary
addpath /home/gc3-user/project_SCR/code/SCRalyze_newVBA_27_10_2014
rmpath('/usr/local/MATLAB/R2014a/toolbox/stateflow') 

%% initiate SCRalyze
scr_init;
global settings

%basic_path = '/home/gc3-user/project_SCR/';
%scrpath = fullfile(basic_path,'data',['Data' suffix]);
%modelpath = fullfile(basic_path,'models',['Models_DCM_newVBA2' suffix]);
%[s,m] = mkdir(modelpath);

% Create result folder
disp('Creating result folder... ');
[s,m] = mkdir(result_path);

% S3IT: what is this all about ?
uni_name   = {'bi','uni'};
indrf_name = {'can','ind'};

% Loading input parameters
disp('Loading input parameters.. ');
load(param_file);

%% start estimation

% Read parameters
s_idx = s_idx;
freq = freq;
uni_idx = uni;
indrf_idx = indrf;
depth_idx = depth;

% import srfile
disp(['Importing data file',data_file,'... ']);
% fn = fullfile(scrpath, [prefix, num2str(s_list(s_idx)) '.mat']); % specify trimmed SCR files
[sts, infos, data] = scr_load_data(data_file, 'events');           % import triggers from trimmed SCR files

events = data{1}.data; % time points of events
cue = events;
outcome = events + 3.5;          % US is followed by CS after 3.5 seconds
events = [cue outcome];

clear options

% set filter frequency
settings.dcm.filter.hpfreq = freq; % see paper

% set options (see scr_dcm.m for description)
options.fig     = 0;
options.dispwin = 0;
options.indrf   = indrf_idx;
options.uni     = uni_idx;
options.depth   = depth_idx;
        
% set file name
% S3IT: wouldn`t help to have an output
% filename that includes `result` or `processed` ?
ofn = fullfile(result_path, [ ...
      'DCM_s', num2str(s_idx), ...
      '_',indrf_name{options.indrf+1}, ...
      '_',num2str(settings.dcm.filter.hpfreq*10000), 'e-4Hz', ...
      '_',uni_name{options.uni+1}, ...
      '_depth' num2str(options.depth) '_newVBA2.mat']);
   
if ~exist(ofn,'file')
   disp('Starting scr_dcm analysis... ');
   tic;
   scr_dcm(data_file, ofn, events, options);
   toc;
else 
   disp('job already done. check dcm_param -> preprocessed!!');
end

disp('Done');
