#! /usr/bin/env python
#
#   ggatk.py -- Front-end script for running ParRecoveryFun Matlab
#   function with a given combination of reference models.
#
#   Copyright (C) 2015  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-11-17:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import ggatk
    ggatk.GgatkScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
import random
import posix

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, \
    hours, minutes, seconds
from gc3libs.workflow import RetryableTask, StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection

gatk_steps_list = ['0','1','2']

# Utility methods
def get_bams(input_folder):
    """
    Return list of .bam .bam.bai pairs
    """
    for bam in [ bams for bams in os.listdir(input_folder) if bams.endswith('.bam')
                 and os.path.isfile(os.path.join(input_folder,bams+".bai"))]:
        yield (os.path.join(input_folder,bam),
               os.path.join(input_folder,bam+".bai"))

def get_vcf_group(input_folder, group_size):
    """
    Return list of .vcf file lists of size `group_size`

    """
    vcf_list = [ os.path.join(input_folder,vcfs) for vcfs in os.listdir(input_folder) \
                 if vcfs.endswith(".vcf") ]
    for i in xrange(0, len(vcf_list), group_size):
        yield (vcf_list[i:i+group_size],i)

## custom application class
class GATKS0Application(Application):
    """
    GATK Stage0: run GATK steps 1,2,3 on a given .bam (and .bai) file
    """
    application_name = 'gatks0'

    def __init__(self, bam_file, bai_file, **extra_args):

        self.S0_output = extra_args['S0_output']

        extra_args['requested_memory'] = extra_args['S0_memory']

        inputs = dict()
        outputs = dict()
        executables = []

        # execution wrapper needs to be added anyway
        gatks0_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gatks0.sh")
        inputs[gatks0_wrapper_sh] = os.path.basename(gatks0_wrapper_sh)

        arguments = "./%s %s %s %s -m %s" % (os.path.basename(gatks0_wrapper_sh),
                                             extra_args['bam_filename'],
                                             extra_args['bai_filename'],
                                             extra_args['sample_name'],
                                             str(extra_args['requested_memory'].amount(conv=int)))

        inputs[bam_file] = extra_args['bam_filename']
        inputs[bai_file] = extra_args['bai_filename']

        # Set output
        self.vcf_output_filename = "./%s.g.vcf" % extra_args['sample_name']
        self.vcfindx_output_filename = "./%s.g.vcf.idx" % extra_args['sample_name']
        outputs[self.vcf_output_filename] = os.path.join(
            self.S0_output,
            self.vcf_output_filename)

        outputs[self.vcfindx_output_filename] = os.path.join(
            self.S0_output,
            self.vcfindx_output_filename)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gatks0.log',
            join=True,
            executables = os.path.basename(gatks0_wrapper_sh),
            **extra_args)

    def terminated(self):
        """
        Verify output
        """
        vcf_output = os.path.join(self.S0_output, self.vcf_output_filename)
        vcf_indx_output = os.path.join(self.S0_output, self.vcfindx_output_filename)
        try:
            assert os.path.isfile(vcf_output), \
                "Output file %s not found." % vcf_output

            assert os.path.isfile(vcf_indx_output), \
                "Output file %s not found." % vcf_indx_output

        except AssertionError as ex:
            self.execution.returncode = (0, posix.EX_OSFILE)
            gc3libs.log.error(ex.message)

class GATKS1Application(Application):
    """
    GATK Stage1: run GATK CombineGVCFs step
    XXX: does the corresponding .vcf.indx file has to be included ? YES
    """
    application_name = 'gatks1'

    def __init__(self, vcf_group, index, **extra_args):

        self.S1_output = extra_args['S1_output']
        extra_args['requested_memory'] = extra_args['S1_memory']

        inputs = dict()
        outputs = dict()
        executables = []

        # execution wrapper needs to be added anyway
        gatks1_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gatks1.sh")
        inputs[gatks1_wrapper_sh] = os.path.basename(gatks1_wrapper_sh)

        arguments = "./%s ./input %s -m %s" % (os.path.basename(gatks1_wrapper_sh),
                                               index,
                                               str(extra_args['requested_memory'].amount(conv=int)))

        for vcf in vcf_group:
            inputs[vcf] = os.path.join('./input',os.path.basename(vcf))

        # Set output
        self.vcf_output_filename = "./combined%d.g.vcf" % index
        outputs[self.vcf_output_filename] = os.path.join(
            self.S1_output,
            self.vcf_output_filename)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gatks1.log',
            join=True,
            executables = os.path.basename(gatks1_wrapper_sh),
            **extra_args)

    def terminated(self):
        """
        Verify output
        """
        vcf_output = os.path.join(self.S1_output, self.vcf_output_filename)
        try:
            assert os.path.isfile(vcf_output), \
                "Output file %s not found." % vcf_output
        except AssertionError as ex:
            self.execution.returncode = (0, posix.EX_OSFILE)
            gc3libs.log.ERROR(ex.message)

class GATKS2Application(Application):
    """
    GATK Stage2: run GATK CombineGVCFs step
    XXX: does the corresponding .vcf.indx file has to be included ?
    """
    application_name = 'gatks2'

    def __init__(self, vcf_list, **extra_args):

        self.S2_output = extra_args['S2_output']
        extra_args['requested_memory'] = extra_args['S2_memory']

        inputs = dict()
        outputs = dict()
        executables = []

        # execution wrapper needs to be added anyway
        gatks2_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gatks2.sh")
        inputs[gatks2_wrapper_sh] = os.path.basename(gatks2_wrapper_sh)

        arguments = "./%s ./input -m %s " % (os.path.basename(gatks2_wrapper_sh),
                                             str(extra_args['requested_memory'].amount(conv=int)))

        for vcf in vcf_list:
            inputs[vcf] = os.path.join('./input',os.path.basename(vcf))

        # Set output
        self.vcf_output = os.path.join(self.S2_output,
                                       'genotyped.gvcf.vcf')
        outputs['genotyped.gvcf.vcf'] = self.vcf_output

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gatks2.log',
            join=True,
            executables = os.path.basename(gatks2_wrapper_sh),
            **extra_args)

    def terminated(self):
        """
        Verify output
        """
        try:
            assert os.path.isfile(self.vcf_output), \
                "Output file %s not found." % vcf_output
        except AssertionError as ex:
            self.execution.returncode = (0, posix.EX_OSFILE)
            gc3libs.log.ERROR(ex.message)

# class GATKStagedTaskCollection(StagedTaskCollection):
#     """
#     Stage0: for each sample run GATK pipeline steps 1,2,3
#     * 1 sample takes 24-72 hours on single core
#     * GATK can be scripted to run individual steps
#     * Output: 2 files per sample (g.vcf and g.vcf.idx size 1GB total)
#     Stage1: Run GATK pipeline steps 4,5 on all g.vcf files together
#     * walltime and memory requirement unknown
#     * steps 4,5 can also be run on groups of g.vcf files but then
#     results need to be combined (GATK step 6)
#     * We will see whether this step is necessary resource-wise
#     * Output: 1 .vcf file size: 50GB
#     Stage2: Filter .vcf file
#     * Run vcftools and own scripts
#     * Output: single .vcf file size: 50MB
#     """
#     def __init__(self, input_bam_folder, **extra_args):
#         self.S0_output = extra_args['S0_output_dir']
#         self.S1_output = extra_args['S1_output_dir']
#         self.input_bam_folder = input_bam_folder
#         self.name = os.path.basename(input_bam_folder)
#         self.extra = extra_args
#         self.output_dir = extra_args['output_dir']
#         StagedTaskCollection.__init__(self)

class GATKSequentialTaskCollection(SequentialTaskCollection):
    def __init__(self, input_bam_folder, **extra_args):
        self.gatkseq = extra_args['gatk_sequence']
        self.S0_output = extra_args['S0_output']
        self.S1_output = extra_args['S1_output']
        self.input_bam_folder = input_bam_folder
        self.name = os.path.basename(input_bam_folder)
        self.extra = extra_args
        self.output_dir = extra_args['output_dir']

        self.jobname = "GATK-" + "_".join(self.gatkseq)

        # Get the first stage task in gatk_sequence
        initial_task_fn = getattr(self, "stage%d" % int(self.gatkseq.pop(0)))
        initial_task = initial_task_fn()

        SequentialTaskCollection.__init__(self, [initial_task])

    def next(self,done):
        """
        self.tasks[done]
        """
        if self.gatkseq:
            try:
                next_gatk = int(self.gatkseq.pop(0))
                next_stage_fn = getattr(self, "stage%d" % next_gatk)
            except AttributeError:
                gc3libs.log.debug("GATK sequence '%s' has no stage%d,"
                                  " ending sequence now.", self, next_gatk)
                self.execution.returncode = self.tasks[done].execution.returncode
                return Run.State.TERMINATED

            # get next stage (2); if we get an error here, something is wrong in
            # the code
            try:
                next_stage = next_stage_fn()
            except AttributeError as err:
                raise AssertionError(
                    "Invalid `Task` instance %r: %s"
                    % (self, str(err)))
            # add next stage to the collection, or end graciously
            if isinstance(next_stage, Task):
                self.add(next_stage)
                return Run.State.RUNNING
            elif isinstance(next_stage, (int, long, tuple)):
                self.execution.returncode = next_stage
                return Run.State.TERMINATED
            else:
                raise AssertionError(
                    "Invalid return value from method `stage%d()` of"
                    " `StagedTaskCollection` object %r: must return `Task`"
                    " instance or number" % (done + 1, self))
        else:
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED


    def stage0(self):
        """
        Stage0: for each sample run GATK pipeline steps 1,2,3
        * 1 sample takes 24-72 hours on single core
        * GATK can be scripted to run individual steps
        * Output: 2 files per sample (g.vcf and g.vcf.idx size 1GB total)
        # 300 samples - see if we can allocate 150 cores for 2 days
        # 1 day each
        Example script:
java -jar -d64 ~/programs/GenomeAnalysisTK.jar\
     -T HaplotypeCaller\
     --emitRefConfidence GVCF\
     -minPruning 3 -stand_call_conf 30 \
     -stand_emit_conf 10 \
     -R ~/goat.genome/goat_scaffoldFG_V1.1.normalised.22.07.fa -I \
        $file -o ${samplename}.g.vcf
        """

        tasks = []

        for (bam_file,bai_file) in get_bams(self.input_bam_folder):
            extra_args = self.extra.copy()
            extra_args['sample_name'] = os.path.basename(bam_file).split('.')[0]
            extra_args['bam_filename'] = os.path.basename(bam_file)
            extra_args['bai_filename'] = os.path.basename(bai_file)
            extra_args['jobname'] = "gatk-s0-%s" % extra_args['bam_filename']

            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        extra_args['jobname'])

            gc3libs.log.debug("Creating Stage0 task for : %s" %
                              (extra_args['bam_filename']))

            tasks.append(GATKS0Application(
                bam_file,
                bai_file,
                **extra_args))

        return ParallelTaskCollection(tasks)


    def stage1(self):
        """
        Start this stage IIF stage0 all completed (i.e. no failures)
        combine all .g.vcf files alltogether
        group in blocks (e.g. 30 out of the total 300)
        * make grouping an option for stage1
        * Use same GATK and goat.genome vesion as in stage0
        Run "combine_gvcf" script
        script can take an arbitrary number of gvc files and prodices
        1 single gvcf file
        end of stage1: 10 .g.vcf files
        if fails - because of heap size - then re-run with more memory
        Walltime: 2days each
        Cores requires: 10 cores
        Memory 500GB memory top - need to check
        memory: 128GB
        Example script:
java -jar  /home/dleigh/GenomeAnalysisTK-3.1-1/GenomeAnalysisTK-3.4-46/GenomeAnalysisTK.jar \
    -T CombineGVCFs \
    -R /home/dleigh/goatgenome/01.GENOME/scaffold/goat_scaffoldFG_V1.1.normalised.22.07.fa \
--variant /home/dleigh/demultiplexed.reads/GATK/GR0766.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1380.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1387.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1390.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1422.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1424.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1440.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1441.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1709.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1728.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1938.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1939.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR1997.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR2001.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR2053.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR2055.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/GR2056.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0038.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0047.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0101.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0242.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0258.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0261.g.vcf \
--variant /home/dleigh/demultiplexed.reads/GATK/SG0306.g.vcf \
-o /home/dleigh/demultiplexed.reads/GATK/combined3.g.vcf

        get list of all outputs in 'outputs0' folder
        group them in 's1_chunk'
        for each group run GATKS1Application
        """
        # XXX: add check if stage0 completed properly
        # Stop otherwise

        tasks = []

        for (vcf_group,index) in get_vcf_group(self.extra['S0_output'],
                                               int(self.extra['S1_group'])):
            extra_args = self.extra.copy()
            extra_args['jobname'] = "gatk-s1-%d" % index

            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        extra_args['jobname'])

            gc3libs.log.debug("Creating Stage1 task for : %d" %
                              index)

            tasks.append(GATKS1Application(
                vcf_group,
                index,
                **extra_args))

        return ParallelTaskCollection(tasks)


    def stage2(self):
        """
        Start this stage IIF stage1 all completed (i.e. no failures)
        * Use same GATK and goat.genome vesion as in stage0
        Combine 10 g.vcf files:
        take 10 + 3 and run GenotypeGVCF
        - add 3 more .g.vcf aggregated files (pased from CLI as option)
        output: 1 .cvf file (in result folder)
        Memory and walltime to be checked
        Example script:
java -jar /home/dleigh/GenomeAnalysisTK-3.1-1/GenomeAnalysisTK-3.4-46/GenomeAnalysisTK.jar \ # specifying the jar script
   -T GenotypeGVCFs \ # command within GATK
   -R /home/dleigh/goatgenome/01.GENOME/scaffold/goat_scaffoldFG_V1.1.normalised.22.07.fa \ # reference genome location
   -stand_call_conf 30 \ # constant
   -stand_emit_conf 10 \ # constant
   --variant /home/dleigh/demultiplexed.reads/GATK/combined.g.vcf \ # the input files
   --variant /home/dleigh/demultiplexed.reads/GATK/combined2.g.vcf \
   --variant /home/dleigh/demultiplexed.reads/GATK/combined3.g.vcf \
   -o /home/dleigh/demultiplexed.reads/GATK/genotyped.gvcf.vcf
        """
        vcfs_list = [ os.path.join(self.extra['S1_output'],vcfs) \
                      for vcfs in os.listdir(self.extra['S1_output'])]
        if 'S2_extra_input' in self.extra and self.extra['S2_extra_input']:
            vcfs_list.extend([ os.path.join(self.extra['S2_extra_input'],vcfs) \
                               for vcfs in os.listdir(self.extra['S2_extra_input']) ])

        extra_args = self.extra.copy()
        extra_args['jobname'] = "gatk-s2"

        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                    extra_args['jobname'])

        gc3libs.log.debug("Creating Stage2 task")

        return GATKS2Application(
            vcfs_list,
            **extra_args)

class GgatkScript(SessionBasedScript):
    """
    For each param file (with '.mat' extension) found in the 'param folder',
    GscrScript extracts the corresponding index (from filename) and searches for
    the associated file in 'data folder'. For each pair ('param_file','data_file'),
    GscrScript generates execution Tasks.

    The ``gscr`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gscr`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            # application = GATKStagedTaskCollection,
            application = Application,
            stats_only_for = Application
            )

    def setup_args(self):

        self.add_param('bam_folder', type=str,
                       help="Location of samples files in .bam format.")

        self.add_param('gatk_sequence', type=str,
                       help="Comma separated list of GATK steps. "
                       "Valid values: %s" % gatk_steps_list)

    def setup_options(self):
        """
        * gatk
        * goat.genome
        """
        self.add_param("-g", "--gatk", metavar="PATH",
                       dest="gatk", default=None,
                       help="Location of alternative GATK jar file.")

        self.add_param("-G", "--goat", metavar="PATH",
                       dest="goat", default=None,
                       help="Location of alternative goat genome folder.")

        self.add_param("-S0O", "--s0-output",
                       metavar="PATH",
                       dest="S0_output",
                       default=os.path.join(os.environ['PWD'],"S0.out"),
                       help="Location were all outputs from Stage0"
                       " will be placed. Default: %(default)s.")

        self.add_param(
            "-S0M", "--s0-memory-per-task", dest="S0_memory",
            type=Memory, default=2 * GB,  # 2 GB
            metavar="GIGABYTES",
            help="Set the amount of memory required per S0 task;"
            " default: %(default)s. Specify this as an integral number"
            " followed by a unit, e.g., '512MB' or '4GB'.")

        self.add_param("-S1O", "--s1-output", metavar="PATH",
                       dest="S1_output",
                       default=os.path.join(os.environ['PWD'],"S1.out"),
                       help="Location were all outputs from Stage1"
                       " will be placed. Default: %(default)s.")

        self.add_param(
            "-S1M", "--s1-memory-per-task", dest="S1_memory",
            type=Memory, default=2 * GB,  # 2 GB
            metavar="GIGABYTES",
            help="Set the amount of memory required per S1 task;"
            " default: %(default)s. Specify this as an integral number"
            " followed by a unit, e.g., '512MB' or '4GB'.")

        self.add_param("-S1G", "--s1-group-factor", metavar="NUM",
                       dest="S1_group", default=30,
                       help="Group size for S1 aggregate task.")

        self.add_param("-S2O", "--s2-output", metavar="PATH",
                       dest="S2_output",
                       default=os.path.join(os.environ['PWD'],"S2.out"),
                       help="Location were all outputs from Stage2"
                       " will be placed. Default: %(default)s.")
        self.add_param(
            "-S2M", "--s2-memory-per-task", dest="S2_memory",
            type=Memory, default=2 * GB,  # 2 GB
            metavar="GIGABYTES",
            help="Set the amount of memory required per S2 task;"
            " default: %(default)s. Specify this as an integral number"
            " followed by a unit, e.g., '512MB' or '4GB'.")

        self.add_param("-S2E", "--s2-extra", metavar="PATH",
                       dest="S2_extra_input", default=None,
                       help="Location of the folder with Stage2 extra input files.")

    def parse_args(self):
        try:
            assert os.path.isdir(self.params.bam_folder), \
                "Input BAM folder %s not found." % self.params.bam_folder
            self.params.bam_folder = os.path.abspath(self.params.bam_folder)

            # make all path options ABS path
            self.params.S0_output = os.path.abspath(self.params.S0_output)
            self.params.S1_output = os.path.abspath(self.params.S1_output)
            self.params.S2_output = os.path.abspath(self.params.S2_output)

            # Check GATK sequence option
            self.params.gatk_sequence = [gatk_seq for gatk_seq in \
                                  self.params.gatk_sequence.split(',')
                                  if gatk_seq in gatk_steps_list]
            assert len(self.params.gatk_sequence) > 0,"Invalid GATK sequence: [%s]. " \
                "Please check input GATK sequence. " \
                "Valid values: %s" % (self.params.gatk_sequence,
                                      gatk_steps_list)
            self.params.gatk_sequence.sort()

            if self.params.S2_extra_input:
                assert os.path.isdir(self.params.S2_extra_input)
                self.params.S2_extra_input = os.path.abspath(self.params.S2_extra_input)

            assert int(self.params.S1_group), \
                "Invalid group size for S1 aggregate task: '%s'." % self.params.S1_group

            if self.params.goat:
                assert os.path.isdir(self.params.goat), \
                    "Location of alternative goat genome folder '%s' not found." \
                    % self.params.goat
                self.params.goat = os.path.abspat(self.params.goat)

        except AssertionError as ex:
            raise ValueError(ex.message)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GscrApplication.

        First loop the input files, then loop the selected benchmarks
        """
        extra_args = extra.copy()
        extra_args.update(self.params.__dict__)
        # extra_args['S0_output_dir'] = os.path.abspath(self.params.S0_output)
        # extra_args['S1_output_dir'] = os.path.abspath(self.params.S1_output)
        # extra_args['S2_output_dir'] = os.path.abspath(self.params.S2_output)
        # extra_args['gatk_sequence'] = self.gatk_sequence
        # if self.params.S2_extra_input:
        #     extra_args['S2_extra_input'] = os.path.abspath(self.params.S2_extra_input)
        # extra_args['S1_group'] = int(self.params.S1_group)
        # if self.params.gatk:
        #     extra_args['gatk'] = self.params.gatk

        # if self.params.goat:
        #     extra_args['goat'] = self.params.goat

        # extra_args['output_dir'] = self.params.output

        # return [GATKStagedTaskCollection(self.params.bam_folder,
        #                                  **extra_args)]

        return [GATKSequentialTaskCollection(self.params.bam_folder,
                                             **extra_args)]
