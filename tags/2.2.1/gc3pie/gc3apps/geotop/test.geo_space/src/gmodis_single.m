function gmodis_single(input_load_data, input_fsc, output_dir)
%% Script to analyze snowline elevations based on MODIS FSC observations
% Input data 
%
%   input_load_data - input folder where input .tif images are to be loaded from
%   input_fsc   - input folder for FSC data
%   output_dir  - output folder

% Snowline data in output grids Zsnow* may take the values:
% 0-8850    : snowline elevation
% -9999     : snow cover too low [mean(fsc)<minfsc]
% 9999      : snow cover too high [mean(fsc)>maxfsc]
% -1        : elevation spread too thin (can occur everywhere, depending on given cloud cover)
% -3        : complete cloud cover


% SPECIFY PARAMETERS
 
step = 10;              % spacing of measurements (in pixels)
windowsize = 21;        % sampling window size (needs to be uneven)
windowshape = 'square';   % shape of window (only 'disk' or 'square' possible)

fprintf('Input arguments \n----------------\n');
fprintf('Step size: %d\n', step);
fprintf('Window size: %d\n', windowsize);
fprintf('Window shape: %s\n', windowshape);
fprintf('Input file: %s\n', input_fsc);
fprintf('FSC load folder: %s\n', input_load_data);
fprintf('Output fodler: %s\n', output_dir);

[dirpath, input_name, extension] = fileparts(input_fsc);

outputFileName = strcat(output_dir, input_name(1:17),'_Zsnow_step_',num2str(step),'_window_',num2str(windowsize),windowshape,'.mat');
fprintf('Output file: %s\n', outputFileName);

fprintf('----------------\n');

startFolder = pwd;

%% LOAD DATA
fprintf('Loading data... ');

% load dem
cd(input_load_data);
dem = geotiffread('gtopo_asia_mosaic_dem500m_int.tif');
asp = geotiffread('gtopo_asia_mosaic_dem500m_aspect_int.tif');

dem = single(dem(1:3847,:));
asp = single(asp(1:3847,:));

% load the lake index data
load('ix_lakes.mat')

fprintf('[ok]\n');

%% MAIN LOOP
cd(startFolder)

load(input_fsc);
clear fscmap; fscmap = single(fsc); clear fsc;
fscmap(ix_lakes) = NaN; % filtering the lakes (GWLD data level 1 = lakes > 50 km2) as these are characterized by atypical snow cover durations

% run snowlInes3.m script
tic
 [SL] = snowlines_block_newpara_single(fscmap,dem,asp,step,windowshape,windowsize);
toc

fprintf('Saving output... ');
% save(strcat(output_dir, input_fsc.name(1:17),'_Zsnow_step_',num2str(step),'_window_',num2str(windowsize),windowshape,'.mat'),'SL');
save(outputFileName,'SL');
fprintf('[ok]\n');

fprintf('[Done]\n\n');
