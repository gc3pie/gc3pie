function estimateDCM(param,data)
% estimate_DCM_from_param_new estimates DCM models using SCRalyze
%
%_____________________________________
% (C) 2013 Matthias Staib (Zurich University Hospital for Psychiatry)
%
% v002 ms 01.12.2013 mstaib, Using data from sound experiment now
% v003 ms 28.10.2014 new VBA version from 27.10.2014


addpath /home/gc3-user/project_SCR/code/SCRalyze_newVBA_27_10_2014
rmpath('/usr/local/MATLAB/R2014a/toolbox/stateflow') 

sound_exp=str2num(sound_exp)

switch sound_exp
    case 0
        suffix = '_color';
        prefix = 'tscr_HRA_1_';
        s_list = 12:31;
    case 1
        suffix = '_sound';
        prefix = 'tscr_soundexp_scbd';
        s_list = 24:35;
    otherwise
	warning('sound_exp value not valid. Must be either 1 or 2')
	return
end

%% initiate SCRalyze
scr_init;
global settings

%basic_path = '/home/gc3-user/project_SCR/';
%scrpath = fullfile(basic_path,'data',['Data' suffix]);
%modelpath = fullfile(basic_path,'models',['Models_DCM_newVBA2' suffix]);
%[s,m] = mkdir(modelpath);

disp('creating result folder... ');
modelpath = fullfile('./results');
[s,m] = mkdir(modelpath);

uni_name   = {'bi','uni'};
indrf_name = {'can','ind'};

% Loading input parameters
disp(['Loading input parameters from file ',param]);
load(param)

%% start estimation

disp('Reading input parametes... ');
% Read parameters
s_idx = s_idx;
freq = freq;
uni_idx = uni;
indrf_idx = indrf;
depth_idx = depth;

% import srfile
% fn = fullfile(scrpath, [prefix, num2str(s_list(s_idx)) '.mat']); % specify trimmed SCR files
disp(['Inporting data file from ',data]);
fn = fullfile(data)
[sts, infos, data] = scr_load_data(fn, 'events');           % import triggers from trimmed SCR files
        
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
ofn = fullfile(modelpath, [ ...
      'DCM_s', num2str(s_list(s_idx)), ...
      '_',indrf_name{options.indrf+1}, ...
      '_',num2str(settings.dcm.filter.hpfreq*10000), 'e-4Hz', ...
      '_',uni_name{options.uni+1}, ...
      '_depth' num2str(options.depth) '_newVBA2.mat']);
disp(['Output filename set to ',ofn]);

if ~exist(ofn,'file')
   disp('Start scr_dcm... ');
   tic
   scr_dcm(fn, ofn, events, options);
   toc
else disp('job already done. check dcm_param -> preprocessed!!')            
end

