#! /usr/bin/env python

"""
Specialized support for computational jobs running GAMESS-US.
"""

# Copyright (C) 2009-2012  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'


import gc3libs
import gc3libs.application
import gc3libs.application.apppot
import gc3libs.exceptions
import os
import os.path
import re


class GamessApplication(gc3libs.Application):

    """
    Specialized `Application` object to submit computational jobs running GAMESS-US.

    The only required parameter for construction is the input file
    name; subsequent positional arguments are additional input files,
    that are added to the `Application.inputs` list, but not otherwise
    treated specially.

    The `verno` parameter is used to request a specific version of
    GAMESS-US; if the default value ``None`` is used, the default
    version of GAMESS-US at the executing site is run.

    Any other keyword parameter that is valid in the `Application`
    class can be used here as well, with the exception of `input` and
    `output`.  Note that a GAMESS-US job is *always* submitted with
    `join = True`, therefore any `stderr` setting is ignored.
    """

    application_name = 'gamess'

    def __init__(self, inp_file_path, *other_input_files, **extra_args):
        input_file_name = os.path.basename(inp_file_path)
        input_file_name_sans = os.path.splitext(input_file_name)[0]
        output_file_name = input_file_name_sans + '.dat'
        # add defaults to `extra_args` if not already present
        extra_args.setdefault('stdout', input_file_name_sans + '.out')
        extra_args.setdefault('tags', list())
        self.verno = extra_args.get('verno', None)
        if self.verno is not None:
            extra_args['tags'].append("APPS/CHEM/GAMESS-%s" % self.verno)
        else:
            # no VERNO specified
            extra_args['tags'].append("APPS/CHEM/GAMESS")
        # `rungms` has a fixed structure for positional arguments:
        # INPUT VERNO NCPUS; if one of them is to be omitted,
        # we use the empty string instead.
        arguments = [
            "rungms",
            input_file_name,
            str(extra_args.get('verno') or ""),
            str(extra_args.get('requested_cores') or "")
        ]
        if 'extbas' in extra_args:
            other_input_files += extra_args['extbas']
            arguments.extend(
                ['--extbas', os.path.basename(extra_args['extbas'])])
        # set job name
        extra_args['jobname'] = input_file_name_sans
        # issue WARNING about memory handling
        if 'requested_memory' in extra_args:
            # XXX: should this be an error instead?
            gc3libs.log.warning(
                "Requested %s of memory per core; depending on how the execution site"
                " handles memory limits, this may lead to an error in the ``ddikick``"
                " startup.  In that case, re-run without memory specification.",
                extra_args['requested_memory'])
        # build generic `Application` obj
        gc3libs.Application.__init__(self,
                                     arguments=arguments,
                                     inputs=[inp_file_path] +
                                     list(other_input_files),
                                     outputs=[output_file_name],
                                     join=True,
                                     # needed by `ggamess`
                                     inp_file_path=inp_file_path,
                                     **extra_args)

    _termination_re = re.compile(
        r'EXECUTION \s+ OF \s+ GAMESS \s+ TERMINATED \s+-?(?P<gamess_outcome>NORMALLY|ABNORMALLY)-?'
        r'|ddikick.x: .+ (exited|quit) \s+ (?P<ddikick_outcome>gracefully|unexpectedly)',
        re.X)

    def terminated(self):
        """
        Append to log the termination status line as extracted from
        the GAMESS '.out' file.

        The job exit code `.execution.exitcode` is (re)set according
        to the following table:

        =====================  ===============================================
        Exit code              Meaning
        =====================  ===============================================
        0                      the output file contains the string \
                               ``EXECUTION OF GAMESS TERMINATED normally``
        1                      the output file contains the string \
                               ``EXECUTION OF GAMESS TERMINATED -ABNORMALLY-``
        2                      the output file contains the string \
                               ``ddikick exited unexpectedly``
        70 (`os.EX_SOFTWARE`)  the output file cannot be read or \
                               does not match any of the above patterns
        =====================  ===============================================

        """
        gc3libs.log.debug("Running GamessApplication post-processing hook...")
        output_dir = self.output_dir
        if self.stdout:
            output_filename = os.path.join(output_dir, self.stdout)
        else:
            output_filename = os.path.join(
                output_dir,
                os.path.splitext(
                    os.path.basename(
                        self.inp_file_path))[0] +
                '.out')
        if not os.path.exists(output_filename):
            # no output file, override exit code if it indicates success
            if self.execution.exitcode == os.EX_OK:
                self.execution.exitcode = os.EX_SOFTWARE
        else:
            # output file exists, start with pessimistic default and
            # override if we find a "success" keyword
            self.execution.exitcode = os.EX_SOFTWARE  # internal software error
            gc3libs.log.debug("Trying to read GAMESS termination status"
                              " off output file '%s' ..." % output_filename)
            output_file = open(output_filename, 'r')
            for line in output_file:
                match = self._termination_re.search(line)
                if match:
                    if match.group('gamess_outcome'):
                        self.execution.info = line.strip().capitalize()
                        if match.group('gamess_outcome') == 'ABNORMALLY':
                            self.execution.exitcode = 1
                        elif match.group('gamess_outcome') == 'NORMALLY':
                            self.execution.exitcode = 0
                        break
                    elif match.group('ddikick_outcome'):
                        self.execution.info = line.strip().capitalize()
                        if match.group('ddikick_outcome') == 'unexpectedly':
                            self.execution.exitcode = 2
                        # If the GAMESS execution terminated normally, this would
                        # have been catched by the test above; otherwise we risk here
                        # setting the exitcode to EX_OK even if GAMESS didn't run at
                        # all, but `ddikick` does not flag it as an error. (Not sure
                        # this can actually happen, but better err on the safe side.)
                        # elif match.group('ddikick_outcome') == 'gracefully':
                        #    gc3libs.log.info('Setting exit code to 0')
                        #    self.execution.exitcode = 0
                        break
                    else:
                        raise AssertionError(
                            "Input line '%s' matched,"
                            " but neither group 'gamess_outcome'"
                            " nor 'ddikick_outcome' did!")
            output_file.close()


class GamessAppPotApplication(GamessApplication,
                              gc3libs.application.apppot.AppPotApplication):

    """
    Specialized `AppPotApplication` object to submit computational
    jobs running GAMESS-US.

    This class makes no check or guarantee that a GAMESS-US executable
    will be available in the executing AppPot instance: the
    `apppot_img` and `apppot_tag` keyword arguments can be used to
    select the AppPot system image to run this application; see the
    `AppPotApplication`:class: for information.

    The `__init__` construction interface is compatible with the one
    used in `GamessApplication`:class:. The only required parameter for
    construction is the input file name; any other argument names an
    additional input file, that is added to the `Application.inputs`
    list, but not otherwise treated specially.

    Any other keyword parameter that is valid in the `Application`
    class can be used here as well, with the exception of `input` and
    `output`.  Note that a GAMESS-US job is *always* submitted with
    `join = True`, therefore any `stderr` setting is ignored.
    """

    application_name = 'gamess'

    def __init__(self, inp_file_path, *other_input_files, **extra_args):
        input_file_name = os.path.basename(inp_file_path)
        input_file_name_sans = os.path.splitext(input_file_name)[0]
        output_file_name = input_file_name_sans + '.dat'
        # add defaults to `extra_args` if not already present
        extra_args.setdefault('stdout', input_file_name_sans + '.out')
        extra_args.setdefault('application_tag', "gamess")
        extra_args.setdefault('tags', list())
        extra_args['tags'].insert(0, "APPS/CHEM/GAMESS-APPPOT-0.11.11.08")
        if 'extbas' in extra_args and extra_args['extbas'] is not None:
            other_input_files += extra_args['extbas']
            arguments.extend(['--extbas', os.path.basename(extbas)])
        # set job name
        extra_args['jobname'] = input_file_name_sans
        # init superclass
        gc3libs.application.apppot.AppPotApplication.__init__(
            self,
            # `rungms` has a fixed structure for positional arguments:
            # INPUT VERNO NCPUS; if one of them is to be omitted,
            # we cannot use the empty string instead because `apppot-start`
            # cannot detect it from the kernel command line, so we have to
            # hard-code default values here...
            arguments=[
                "localgms",
                input_file_name,
                str(extra_args.get('verno') or "00"),
                str(extra_args.get('requested_cores') or "")
            ],
            inputs=[inp_file_path] + list(other_input_files),
            outputs=[output_file_name],
            join=True,
            # needed by `ggamess`
            inp_file_path=inp_file_path,
            **extra_args)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="gamess",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
