%% create parameter file (options, frequency) + check if data already exists
%
%_____________________________________
% (C) 2013 Matthias Staib (Zurich University Hospital for Psychiatry)
%
% v001 ms 01.07.2013 initial version
% v002 ms 28.10.2014 new VBA version from 27.10.2014


clear all;
clc

basic_path = '/home/gc3-user/project_SCR/';

sound_exp = input('Dataset: Sound (1) or Color (0): ');
switch sound_exp
    case 0
        suffix = '_color';
        s_list = 12:31;
        trials     = 180;   % how many trials should be used for dcm (max 180)
    case 1
        suffix = '_sound';
        s_list = 24:34;
        trials     = 128;
end

scrpath = fullfile(basic_path,'data',['Data' suffix]);
modelpath = fullfile(basic_path,'models',['Models_DCM_newVBA2' suffix]);
[s,m] = mkdir(modelpath);

%% default: set filter frequencies

filtfreq_all     = [.005 .0159 .02:.005:.1]; % all tested filter settings in paper
% filtfreq_all     = [.005 .02:.02:.1]; % all tested filter settings in paper
filtfreq_default = 0.0159;                      % default filter setting of SCRalyze
filtfreq_posneu  = 0.035;                       % optimal filter settings for pos > neutral (experiment 2)
filtfreq_negneu  = [0.05 0.06];                 % optimal filter settings for neg > neutral (experiment 2)
filtfreq_newlow  = [0.0001 0.001 0.012];              % new low frequencies
filtfreq_newlow  = [0.0001 0.012];              % new low frequencies

% set frequency for current estimation
filtfreq = filtfreq_default;

%% default: set other parameter settings (naming for files and values for estimation)

uni_list   = [  0]; % filter uni_name:  0=bi, 1=uni (default: 0)
indrf_list = [  0]; % response function: 0=canonical, 1=individual (default: 0)
depth_list = [2  ]; % number of trials used for estimation at the same time (default: 2)

uni_name   = {'bi','uni'};
indrf_name = {'can','ind'};

%% start estimation
i=0;
for s_idx = 1:numel(s_list) % subjects
    
    for filtfreq_idx = 1:numel(filtfreq)   % filter frequency
        for uni_idx = uni_list             % filter direction
            for indrf_idx = indrf_list     % response function
                for depth_idx = depth_list % number of trials / depth
                    
                    clear options
                    
                    % set filter frequency
                    
                    settings.dcm.filter.hpfreq = filtfreq(filtfreq_idx); % see paper

                    % set options (see scr_dcm.m for description)
                    
                    options.indrf   = indrf_idx;
                    options.uni     = uni_idx;
                    options.depth   = depth_idx;
                    
                    
                    % set file
                    % ofn = fullfile(modelpath, ['DCM_s', num2str(s_list(s_idx)), ...
                    %    '_',indrf_name{options.indrf+1}, ...
                    %    '_',num2str(settings.dcm.filter.hpfreq*10000), 'e-4Hz', ...
                    %    '_',uni_name{options.uni+1}, ...
                    %    '_depth' num2str(options.depth) '_newVBA.mat']);
                    
		    s3it_ofn = ['DCM_s', num2str(s_list(s_idx)), ...
                        '_',indrf_name{options.indrf+1}, ...
                        '_',num2str(settings.dcm.filter.hpfreq*10000), 'e-4Hz', ...
                        '_',uni_name{options.uni+1}, ...
                        '_depth' num2str(options.depth) '_newVBA.mat'];
 
                    if ~exist(s3it_ofn,'file')
                        
                        disp(s3it_ofn)
                        i=i+1;
                        % file_names{i} = ofn;
                        % param.s_idx{i} = s_idx;
                        % param.indrf{i} = options.indrf;
                        % param.freq{i} = settings.dcm.filter.hpfreq;
                        % param.uni{i} = options.uni;
                        % param.depth{i} = options.depth;
                    
			indrf = indrf_idx;
			freq = settings.dcm.filter.hpfreq;
			uni = uni_idx;
			depth = depth_idx;
    
			% S3IT: instead of packing parameters and filenames into a single param file
			% we create 1 file for each of the s_idx
			% The created file will be used by 'estimate' function directly
			save(fullfile(basic_path,[s3it_ofn]), 's_idx','indrf','freq','uni','depth')	

                    end
                end
            end
        end
    end
end;

%if exist('file_names','var')
%    processed       = zeros(1,size(file_names,2));
%    processing_time = zeros(1,size(file_names,2));
%    disp(numel(processed))
%    save(fullfile(basic_path,['dcm_param_newVBA2' suffix '.mat']),'file_names','param','processed','trials','processing_time')
%end
