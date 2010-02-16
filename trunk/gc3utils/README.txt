Grid GAMESS utilities

Webpage: none yet

===


Installation
======================

If you checked out this directory to your $HOME:

Add the $HOME/gc3utils dir to your $PYTHONPATH environment variable, and $HOME/gc3utils/tools to your $PATH.  To make it permanent, put this in your ~/.bashrc file:

export PYTHONPATH=$HOME/gc3utils:$PYTHONPATH
export PATH=$HOME/gc3utils/tools:$PATH


Require configuration
======================

1) in $HOME create folder named .gc3
	cd
	mkdir .gc3

2) create config file for each resource you can access
	example of config file:

	cat $HOME/.gc3/config

-------------------------------
[DEFAULT]
lrms_jobid = .lrms_jobid
lrms_log = .lrms_log
lrms_finished = .finished
debug = 0

[unibe]
type = arc
frontend = smscg.unibe.ch
walltime = 12
cores = 4
applications = gamess

[smscg]
type = arc
frontend = 
walltime = 12
cores = 2
applications = gamess

[idgc3]
type = arc
frontend = idgc3grid01.uzh.ch
walltime = 12
cores = 4
applications= gamess

[ocikbpra]
type = arc
frontend = ocikbpra.unizh.ch
walltime = 12
cores = 2
applications = gamess

-------------------------------

	Note: please do not change [DEFAULT] section unless you know what you are doing
	Note2: at the moment only ARC backend is functioning
	Note3: in an arc resource, is frontend is left blank (like for smscg in the example) the entire infrastructure will be used

3) create aaicredentail file containing your AAI username
	cat $HOME/.gc3/aaicredential

	m......

4) create gamess_template.xrsl file
	 cat ~/.gc3/gamess_template.xrsl 

&(executable="$GAMESS_LOCATION/nggms")
(arguments="INPUT_FILE_NAME")
(jobname=GAMESS_INPUT_FILE_NAME) 
(stdout=out) 
(stderr=err)
(count="CORES")
(memory="MEMORY")
(cputime="WALLTIME")
(inputFiles=(INPUT_FILE_NAME.inp "INPUT_FILE_PATH/INPUT_FILE_NAME.inp"))
(outputFiles=(INPUT_FILE_NAME.dat ""))
(runTimeEnvironment=APPS/CHEM/GAMESS-2009)	

	Note: please do not change the template unless you know what you are doing


How to use gridgamess suite
===========================


5) Submit gamess job:
	gsub gamess <location of input file>

	Example:  gsub gamess /home/mpackard/gamess/inputs/exam01.inp

	Note: if no resource is specified, gsub will simply pick the first one in the list
	Note2: use "-r <resource_name>" as an additional parameter for forcing submission to a given resource
	Example:  gsub -r ocikbpra gamess /home/mpackard/gamess/inputs/exam01.inp

5.1) gsub returns a jobid which is a foldername created as follow:
	$PWD/<internal_coding>

	Note: PWD is evaluated at the moment of gsub, so make sure it points to a location where you have write access
	Note2: suggestion: create a gamess_jobs folder and submit from there (cd gamess_jobs; gsub .... )

6) Use the returned jobid for checking the status of the gamess execution with gstat
	gstat <jobid>

6.1) gstat returns either RUNNING or FINISHED


7) Once job is FINISHED retrieve results with gget
	gget <jobid>

7.1) if gget completes successfully, results are stored in the jobid folder

