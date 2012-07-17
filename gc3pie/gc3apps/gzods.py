#!/usr/bin/python

if __name__ == '__main__':
	import gzods
	gzods.ZodsScript().run()

from gc3libs.cmdline import SessionBasedScript
import gc3libs
import os
from xml.dom import minidom

class GzodsApp(gc3libs.Application):
    """
This application will run ZODS input files on distributed resources 
and retrive the output in subdirectories named Zods* 
inside the current directory.
Example run on 5 cores:
./gzods dirWithInputFiles -c 5 -N    
"""
    def __init__(self, filename,**kw):
        (input1, input2) = self.check_input(filename)
        
	self.ListOfFiles = [] 
	self.ListOfFiles.append(filename)
	gc3libs.log.info("List of detected input files %s", self.ListOfFiles)
	gc3libs.Application.__init__(
            self,
            tags=["APPS/CHEM/ZODS-01"],
	    executable = '$MPIRUN', # mandatory
            arguments = ["-n", kw['requested_cores'], '$ZODS_BINDIR/simulator','input.xml'],                
            inputs = [filename, input1, input2],                  # mandatory, inputs are files that will be copied to the site
            outputs = ['zods.txt'],                 # mandatory
            stderr = "stderr.txt",
	    stdout = "zods.txt",
	    **kw	)


    def terminated(self):
        filename = os.path.join(self.output_dir, self.stdout)
        gc3libs.log.info("FILENAME %s", filename)
        gc3libs.log.info("TERMINATED")


# Detect the following references to external files in input.xml
#   <average_structure>
#      <file format="cif" name="californium_simple_3.cif"/>
#   </average_structure>
# <reference_intensities file_format="xml" file_name="data.xml"/>
 
    def check_input(self,filename):
	gc3libs.log.info("Checking the input file: %s", filename)
	DOMTree = minidom.parse(filename)
	cNodes = DOMTree.childNodes
	if len(cNodes[0].getElementsByTagName('reference_intensities')) > 0 and len(cNodes[0].getElementsByTagName('average_structure')) > 0:
		data_file = cNodes[0].getElementsByTagName('reference_intensities')[0].getAttribute('file_name')
		avg_file = cNodes[0].getElementsByTagName('average_structure')[0].getElementsByTagName('file')[0].getAttribute('name')
        	if os.path.exists(avg_file)  == False or os.path.exists(data_file) == False:
			gc3libs.log.debug("Averaged structure file %s or reference intensities file %s DO NOT exist.", avg_file, data_file)
			return ("","")
		else:
			gc3libs.log.info("Averaged structure file %s or reference intensities file %s DO exist.", avg_file, data_file)
			return (data_file, avg_file)
	else:
		gc3libs.log.debug("The input file is NOT a valid XML file for ZODS application")
		return False
	
class ZodsScript(SessionBasedScript):
	"""
	Help Test.
	"""
	version = "1.0"
	pass

	def new_tasks(self, extra):
	   tasks=[]
	   #mydir = self.params.args[0]
 	   self.log.info("Analayzed dirs =%s", self.params.args)
	   listOfFiles = self._search_for_input_files(self.params.args, pattern="input.xml")
	   gc3libs.log.info("ListOfFiles in NEW TASK: %s", listOfFiles) 
	   for i, filename in enumerate(listOfFiles):
		print filename
		tasks.append(("Zods"+str(i), GzodsApp, [filename], extra.copy())) 	 
	   return tasks

