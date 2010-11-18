#! /usr/bin/env python
#
"""
Specialized support for computational jobs running programs in the Rosetta suite.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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
__version__ = '$Revision$'


import gc3libs
import gc3libs.application
from gc3libs.Exceptions import *
import os
import os.path
from pkg_resources import Requirement, resource_filename


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
        # ensure `application` has no trailing ".something' (e.g., ".linuxgccrelease")
        application = os.path.splitext(application)[0]
        
        _inputs = list(inputs.values())
        gc3utils.log.debug("RosettaApplication: _inputs=%s" % _inputs)
        _outputs = list(outputs) # make a copy

        # do specific setup required for/by the support script "rosetta.sh"
        src_rosetta_sh = resource_filename(Requirement.parse("gc3utils"), 
                                           "gc3utils/etc/rosetta.sh")
        gc3utils.log.debug("RosettaApplication: src_rosetta_sh=%s" % src_rosetta_sh)
        rosetta_sh = application + '.sh'
        _inputs.append((src_rosetta_sh, rosetta_sh))
        _outputs.append(application + '.log')
        _outputs.append(application + '.tar.gz')

        _arguments = [ ]
        for opt, file in inputs.items():
            _arguments.append(opt)
            _arguments.append(os.path.basename(file))

        if flags_file:
            _inputs.append((flags_file, application + '.flags'))
            # the `rosetta.sh` driver will do this automatically
            #_arguments.append("@" + os.path.basename(flags_file))

        if database:
            _inputs.append(database)
            _arguments.append("-database")
            _arguments.append(os.path.basename(database))

        if len(arguments) > 0:
            _arguments.extend(arguments)

        kw['application_tag'] = 'rosetta'
        if kw.has_key('tags'):
            kw['tags'].append("APPS/BIO/ROSETTA-3.1")
        else:
            kw['tags'] = [ "APPS/BIO/ROSETTA-3.1" ]

        kw.setdefault('stdout', application+'.stdout.txt')
        kw.setdefault('stderr', application+'.stderr.txt')

        Application.__init__(self,
                             executable = "./%s" % rosetta_sh,
                             arguments = _arguments,
                             inputs = _inputs,
                             outputs = _outputs,
                             **kw)

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
            inputs = { 
                "-in:file:s":pdb_file_path,
                "-in:file:native":native_file_path,
                },
            outputs = [
                pdb_file_name_sans + '.fasc',
                pdb_file_name_sans + '.sc',
                ],
            flags_file = flags_file,
            arguments = [ 
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
