Grid GAMESS utilities

Webpage: none yet

===


Installation
======================

If you checked out this directory to your $HOME:

Add the $HOME/gc3utils dir to your $PYTHONPATH environment variable, and $HOME/gc3utils/tools to your $PATH.  To make it permanent, put this in your ~/.bashrc file:

export PYTHONPATH=$HOME/gc3utils:$PYTHONPATH
export PATH=$HOME/gc3utils/tools:$PATH


To install Nordugrid Python libs in Debian 5 (Lenny):

(Adapted from here: http://download.nordugrid.org/repos.html)

Add to /etc/apt/sources.list:
deb http://download.nordugrid.org/repos/debian/ lenny main
deb-src http://download.nordugrid.org/repos/debian/ lenny main

Modify /etc/apt/preferences:
Package: *
Pin: release a=stable
Pin-Priority: 900

Package: *
Pin: release o="NorduGrid"
Pin-Priority: 100

Then run:
# wget -q http://download.nordugrid.org/DEB-GPG-KEY-nordugrid.asc -O- | sudo apt-key add -
# gpg --search-keys "mattias ellert"
# aptitude update
# aptitude install nordugrid-arc-python 

Be sure to add it to your PYTHONPATH:

# export PYTHONPATH=$PYTHONPATH:/opt/nordugrid/lib/python2.5/site-packages



Require configuration
======================

1) in $HOME create folder named .gc3
	cd
	mkdir .gc3

2) create config file for each resource you can access
	example of config file:

	cat $HOME/.gc3/config

-------------------------------
# Note: do not change the DEFAULT values unless you know what you are doing
[DEFAULT]
lrms_jobid = .lrms_jobid
lrms_log = .lrms_log
lrms_finished = .finished
debug = 0

# Template of a resouce block
# [Resource_name]
# type = <arc,ssh>
# frontend = <hostname>   Name of the fronend node to be contacted
# walltime = <int>        Maximum walltime in HOURS the resource ca allow for a
#                         single job
# ncores = <int>          Maximun number of cores a single job can require
# applications = gamess 
# memory_per_core = <int> Maximum number of memory in GB a single job can require per
#                         allocated core
# gamess_location = <Path_to_local_gamess_location>

[unibe]
type = arc
frontend = smscg.unibe.ch
walltime = 12
ncores = 4
applications = gamess
memory_per_core = 1

[smscg]
type = arc
frontend = 
walltime = 12
ncores = 2
applications = gamess
memory_per_core = 1

[idgc3]
type = arc
frontend = idgc3grid01.uzh.ch
walltime = 12
ncores = 8
applications= gamess
memory_per_core = 2

[ocikbpra]
type = arc
frontend = ocikbpra.unizh.ch
walltime = 12
ncores = 2
applications = gamess
memory_per_core = 1

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
(stdout=INPUT_FILE_NAME.stdout) 
(stderr=INPUT_FILE_NAME.stderr)
(gmlog="gmlog")
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




