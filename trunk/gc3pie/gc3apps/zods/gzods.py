#!/usr/bin/env python
# summary of user-visible changes
__changelog__ = """
  2012-07-17:
    * Initial releases.
"""
__author__ = 'Lukasz Miroslaw <lukasz.miroslaw@uzh.ch>'
__docformat__ = 'reStructuredText'

if __name__ == '__main__':
	import gzods
	gzods.ZodsScript().run()

import os
import xml.dom.minidom 
import gc3libs
import gc3libs.cmdline 

class GzodsApp(gc3libs.Application):
    """
    This class is derived from gc3libs.Application and defines ZODS app with its input and output files. 
    """
    def __init__(self, filename,**kw):
	if self.check_input(filename) == None:
		gc3libs.log.warning("Input files for ZODS app was not detected.")
		return None
	(input1, input2) = self.check_input(filename)
	input1 = os.path.abspath(input1)
	input2 = os.path.abspath(input2)
	filename = filename
	#filename = os.path.abspath(filename)
	gc3libs.log.debug("Detected input files for ZODS: %s, %s and %s.", filename, input1, input2)
	gc3libs.Application.__init__(
            self,
            tags=["APPS/CHEM/ZODS-01"],
	    executable = '$MPIRUN', # mandatory
            arguments = ["-n", kw['requested_cores'], '$ZODS_BINDIR/simulator',filename],                
            inputs = [filename, input1, input2],    # mandatory, inputs are files that will be copied to the site
            outputs = gc3libs.ANY_OUTPUT,                 # mandatory
	    stderr = "stderr.txt",
	    stdout = "stdout.txt",
	    **kw	)


    def terminated(self):
        filename = os.path.join(self.output_dir, self.stdout)
        gc3libs.log.debug("ZODS single job based on %s has TERMINATED", filename)
	for output in self.outputs:
		gc3libs.log.debug("Retrieved the following file from ZODS job %s", output) 

# Detect the following references to external files in input.xml
#   <average_structure>
#      <file format="cif" name="californium_simple_3.cif"/>
#   </average_structure>
# <reference_intensities file_format="xml" file_name="data.xml"/>
 
    def check_input(self,filename):
	if os.path.exists(filename) == False:
		gc3libs.log.warning("The file %s DOES NOT exists.", filename)
		return None
	basedir = os.path.dirname(filename)
	DOMTree = xml.dom.minidom.parse(filename)
	cNodes = DOMTree.childNodes
	if len(cNodes[0].getElementsByTagName('reference_intensities')) > 0 and len(cNodes[0].getElementsByTagName('average_structure')) > 0:
		data_file = cNodes[0].getElementsByTagName('reference_intensities')[0].getAttribute('file_name')
		avg_file = cNodes[0].getElementsByTagName('average_structure')[0].getElementsByTagName('file')[0].getAttribute('name')
		data_file = os.path.join(basedir,data_file)
		avg_file = os.path.join(basedir,avg_file)
		gc3libs.log.debug("%s references to the following files: %s and  %s.", filename, avg_file, data_file)
		if os.path.exists(avg_file)  == False or os.path.exists(data_file) == False:
			gc3libs.log.warning("Averaged structure file %s or reference intensities file %s DO NOT exist.", avg_file, data_file)
			return None
		else:
			gc3libs.log.debug("Averaged structure file %s or reference intensities file %s DO exist.", avg_file, data_file)
			return (data_file, avg_file)
	else:
		gc3libs.log.debug("The input file is NOT a valid XML file for ZODS application.")
		return None
	
class ZodsScript(gc3libs.cmdline.SessionBasedScript):
	"""
Thiss application will run ZODS app on distributed resources.
The script scans the specified INPUTDIR directories recursively for 'input*.xml' files,
and submits a ZODS job for each input file found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the same directory where the '.xml' file is.
Example run on 5 cores:
./gzods input.xml -c 5
TODO: right now only input file is accepted as an argument. Directories are not accepted because ZODS does not run when the full path to the input file is provided.
TODO: in new_tasks() the program should exit when the argument list is correct. Below self.params.args is empty, None object is returned and gc3pie is still running
	"""
	version = "1.0"

	def new_tasks(self, extra):
	   tasks=[]
           #self.output_dir = os.path.relpath(kw.get('output_dir'))
	   if self.params.args is None or len(self.params.args) == 0:
 	   	self.log.warning("Please specify the directory with input files")
		return None		
 	   self.log.info("Analayzed dirs =%s", self.params.args)
	   listOfFiles = self._search_for_input_files(self.params.args, pattern="input*.xml")
	   gc3libs.log.debug("List of detected input files for ZODS: %s", listOfFiles) 
	   for i, filename in enumerate(listOfFiles):
		tasks.append(("Zods"+str(i), GzodsApp, [filename], extra.copy())) 	 
	   return tasks

