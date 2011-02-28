#! /usr/bin/env python
#
"""
Specialized support for computational jobs running GAMESS-US.
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
__version__ = '1.0rc2 (SVN $Revision$)'


import gc3libs
import gc3libs.application
import gc3libs.exceptions
from gc3libs.InformationContainer import *
import os
import os.path
import re


class GamessApplication(gc3libs.Application):
    """
    Specialized `Application` object to submit computational jobs running GAMESS-US.

    The only required parameter for construction is the input file
    name; any other argument names an additional input file, that
    is added to the `Application.inputs` list, but not otherwise 
    treated specially.

    Any other kyword parameter that is valid in the `Application`
    class can be used here as well, with the exception of `input` and
    `output`.  Note that a GAMESS-US job is *always* submitted with
    `join = True`, therefore any `stderr` setting is ignored.
    """
    def __init__(self, inp_file_path, *other_input_files, **kw):
        input_file_name = os.path.basename(inp_file_path)
        input_file_name_sans = os.path.splitext(input_file_name)[0]
        output_file_name = input_file_name_sans + '.dat'
        # add defaults to `kw` if not already present
        def set_if_unset(key, value):
            if not kw.has_key(key):
                kw[key] = value
        set_if_unset('stdout', input_file_name_sans + '.out')
        set_if_unset('application_tag', "gamess")
        if kw.has_key('tags'):
            kw['tags'].append("APPS/CHEM/GAMESS-2010")
        else:
            kw['tags'] = [ "APPS/CHEM/GAMESS-2010" ]
        arguments = [ input_file_name ] + (kw.get('arguments') or [ ])
        if kw.has_key('arguments'):
            del kw['arguments']
        # build generic `Application` obj
        # set job name
        kw['job_name'] = input_file_name_sans
        gc3libs.Application.__init__(self, 
                                     executable = "$GAMESS_LOCATION/nggms",
                                     arguments = arguments,
                                     inputs = [ (inp_file_path, input_file_name) ] + list(other_input_files),
                                     outputs = [ output_file_name ],
                                     join = True,
                                     # needed by `ggamess`
                                     inp_file_path = inp_file_path,
                                     **kw)

    _termination_re = re.compile(r'EXECUTION \s+ OF \s+ GAMESS \s+ TERMINATED \s+-?(?P<gamess_outcome>NORMALLY|ABNORMALLY)-?'
                                 r'|ddikick.x: .+ (exited|quit) \s+ (?P<ddikick_outcome>gracefully|unexpectedly)', re.X)

    def postprocess(self, output_dir):
        """
        Append to log the termination status line as extracted from
        the GAMESS '.out' file.  According to the normal/abnormal
        termination of the GAMESS job, set the output code to 0 or 1,
        or 2 if the fault was reported only by ``ddikick``.
        """
        gc3libs.log.debug("Running GamessApplication post-processing hook...")
        output_filename = os.path.join(output_dir,
                                       os.path.splitext(os.path.basename(self.inp_file_path))[0] + '.out')
        if os.path.exists(output_filename):
            gc3libs.log.debug("Trying to read GAMESS termination status off output file '%s' ..." % output_filename)
            output_file = open(output_filename, 'r')
            for line in output_file.readlines():
                match = self._termination_re.search(line)
                if match:
                    if match.group('gamess_outcome'):
                        self.execution.info = line.strip().capitalize()
                        if self.execution.exitcode == 0 and match.group('gamess_outcome') == 'ABNORMALLY':
                            self.execution.exitcode = 1
                        break
                    elif match.group('ddikick_outcome'):
                        self.execution.info = line.strip().capitalize()
                        if self.execution.exitcode == 0 and match.group('ddikick_outcome') == 'unexpectedly':
                            self.execution.exitcode = 2
                        break
                    else:
                        raise AssertionError("Input line '%s' matched, but neither group 'gamess_outcome' nor 'ddikick_outcome' did!")
            output_file.close()

                             
    def qgms(self, resource, **kw):
        """
        Return a `qgms` invocation to run GAMESS-US with the
        parameters embedded in this object.
        """
        try:
            qgms = os.path.join(resource.gamess_location, 'qgms')
        except AttributeError:
            raise gc3libs.exceptions.ConfigurationError("Missing configuration parameter `gamess_location` on resource '%s'.",
                                     resource.name)

        if self.requested_walltime:
            # XXX: should this be an error instead?
            gc3libs.log.warning("Requested %d hours of wall-clock time,"
                                " but setting running time limits is not supported by the `qgms` script."
                                " Ignoring request, GAMESS job will be submitted with unspecified running time.", 
                                self.requested_walltime)
        if self.requested_memory:
            # XXX: should this be an error instead?
            gc3libs.log.warning("Requested %d Gigabytes of memory per core,"
                                " but setting memory limits is not supported by the `qgms` script."
                                " Ignoring request, GAMESS job will be submitted with unspecified memory requirements.", 
                                self.requested_memory)
        if self.requested_cores:
            qgms += ' -n %d' % self.requested_cores
        # silently ignore `self.job_name`: `qgms` will set it to a default

        # finally, add the input files
        qgms += ' ' + str.join(" ", [ os.path.basename(r) for r in self.inputs.values() ])
        return (qgms, None)


    # XXX: Assumes `qgms` is the correct way to run GAMESS on *any* batch system...
    qsub = qgms

    
    def cmdline(self, resource, **kw):
        raise NotImplementedError("There is currently no way of running GAMESS directly from the command-line."
                                  " GAMESS invocation requires too many deployment-specific parameters"
                                  " to make a generic invocation script possible.")

gc3libs.application.register(GamessApplication, 'gamess')



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="gamess",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
