#! /usr/bin/env python
#
"""
Specialized support for computational jobs running programs in the Rosetta suite.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '1.0rc1 (SVN $Revision$)'


import gc3libs
import gc3libs.application
from gc3libs.exceptions import *
import os
import os.path
from pkg_resources import Requirement, resource_filename
import tarfile


class RosettaApplication(gc3libs.Application):
    """
    Specialized `Application` object to submit one run of a single
    application in the Rosetta suite.
    
    Required parameters for construction:

      * `application`: name of the Rosetta application to call (e.g., "docking_protocol" or "relax")
      * `inputs`: a `dict` instance, keys are Rosetta ``-in:file:*`` options, values are the (local) path names of the corresponding files.  (Example: ``inputs={"-in:file:s":"1brs.pdb"}``) 
      * `outputs`: list of output file names to fetch after Rosetta has finished running.
    
    Optional parameters:

      * `flags_file`: path to a local file containing additional flags for controlling Rosetta invocation; if `None`, a local configuration file will be used. 
      * `database`: (local) path to the Rosetta DB; if this is not specified, then it is assumed that the correct location will be available at the remote execution site as environment variable ``ROSETTA_DB_LOCATION``
      * `arguments`: If present, they will be appended to the Rosetta application command line.
    """
    def __init__(self, application, inputs, outputs=[], 
                 flags_file=None, database=None, arguments=[], **kw):

        # we're submitting Rosetta jobs thorugh the support script
        # "rosetta.sh", so do the specific setup tailored to this
        # script' usage
        src_rosetta_sh = resource_filename(Requirement.parse("gc3utils"), 
                                           "gc3libs/etc/rosetta.sh")

        # ensure `application` has no trailing ".something' (e.g., ".linuxgccrelease")
        # because it depends on the execution platform/compiler
        # XXX: this does not currently work as intended, since the
        # rosetta.sh script encodes the '.linuxgccrelease' suffix, but
        # we shall eventually move it into the RTE
        if application.endswith('release'):
            application = os.path.splitext(application)[0]
        # remember the protocol name for event methods
        self.__protocol = application

        _inputs = gc3libs.Application._io_spec_to_dict(inputs)

        # since ARC/xRSL does not allow wildcards in the "outputFiles"
        # line, and Rosetta can create ouput files whose number/name
        # is not known in advance, the support script will create a
        # tar archive all of all the output files; therefore, the
        # GC3Libs Application is only told to fetch two files back,
        # and we extract output files back during the postprocessing stage
        _outputs = [ 
            self.__protocol + '.log',
            self.__protocol + '.tar.gz' 
            ]

        # if len(outputs) > 0:
        if outputs:
            _arguments = ['--tar', str.join(' ', [ str(o) for o in outputs ])]
        else:
            _arguments = ['--tar', '*.pdb *.sc *.fasc']
        # XXX: this is too gdocking-specific!
        # for opt, file in _inputs.items():
        #     _arguments.append(opt)
        #     _arguments.append(os.path.basename(file))

        if flags_file:
            _inputs[flags_file] = self.__protocol + '.flags'
            # the `rosetta.sh` driver will do this automatically:
            #_arguments.append("@" + os.path.basename(flags_file))

        if database:
            _inputs[database] = os.path.basename(database)
            _arguments.append("-database")
            _arguments.append(os.path.basename(database))

        #if len(arguments) > 0:
        if arguments:   
            _arguments.extend(arguments)

        kw['application_tag'] = 'rosetta'
        if kw.has_key('tags'):
            kw['tags'].append("APPS/BIO/ROSETTA-3.1")
        else:
            kw['tags'] = [ "APPS/BIO/ROSETTA-3.1" ]

        kw.setdefault('stdout', application+'.stdout.txt')
        kw.setdefault('stderr', application+'.stderr.txt')

        rosetta_sh = self.__protocol + '.sh'
        _inputs[src_rosetta_sh] = rosetta_sh

        gc3libs.Application.__init__(
            self,
            executable = "%s" % rosetta_sh,
            arguments = _arguments,
            inputs = _inputs,
            outputs = _outputs,
            **kw)

    def postprocess(self, output_dir):
        """
        Extract output files from the tar archive created by the
        'rosetta.sh' script.
        """
        tar_file_name = os.path.join(output_dir, 
                                     self.__protocol + '.tar.gz')
        if os.path.exists(tar_file_name):
            if tarfile.is_tarfile(tar_file_name):
                try:
                    tar_file = tarfile.open(tar_file_name, 'r:gz')
                    tar_file.extractall(path=output_dir)
                    os.remove(tar_file_name)
                except tarfile.TarError, ex:
                    gc3libs.log.error("Error extracting output from archive '%s': %s: %s"
                                      % (tar_file_name, ex.__class__.__name__, str(ex)))
            else:
                gc3libs.log.error("Could not extract output archive '%s':"
                                  " format not handled by Python 'tarfile' module" 
                                  % tar_file_name)
        else:
            gc3libs.log.error("Could not find output archive '%s' for Rosetta job" 
                              % tar_file_name)
                

gc3libs.application.register(RosettaApplication, 'rosetta')


class RosettaDockingApplication(RosettaApplication):
    """
    Specialized `Application` class for executing a single run of the
    Rosetta "docking_protocol" application.

    Currently used in the `grosetta` app.
    """
    def __init__(self, pdb_file_path, native_file_path=None, 
                 number_of_decoys_to_create=1, flags_file=None, **kw):
        pdb_file_name = os.path.basename(pdb_file_path)
        pdb_file_dir = os.path.dirname(pdb_file_path)
        pdb_file_name_sans = os.path.splitext(pdb_file_name)[0]
        if native_file_path is None:
            native_file_path = pdb_file_path
        def get_and_remove(D, k, d):
            if D.has_key(k):
                result = D[k]
                del D[k]
                return result
            else:
                return d
        RosettaApplication.__init__(
            self,
            application = 'docking_protocol',
            inputs = [
                pdb_file_path,
                native_file_path,
                ],
            outputs = [
                ],
            flags_file = flags_file,
            arguments = [ 
                "-in:file:s", os.path.basename(pdb_file_path),
                "-in:file:native", os.path.basename(native_file_path),
                "-out:file:o", pdb_file_name_sans,
                "-out:nstruct", number_of_decoys_to_create,
                ] + get_and_remove(kw, 'arguments', []),
            output_dir = get_and_remove(kw, 'output_dir', pdb_file_dir),
            **kw)

gc3libs.application.register(RosettaDockingApplication, 'docking_protocol')


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="rosetta",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
