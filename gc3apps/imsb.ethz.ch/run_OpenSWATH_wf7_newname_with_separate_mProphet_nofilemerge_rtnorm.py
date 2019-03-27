from __future__ import absolute_import, print_function
from fnmatch import fnmatch
import os
import sys


import gc3libs
from gc3libs.workflow import ParallelTaskCollection, StagedTaskCollection
import gc3libs.utils


## local configuration constants

CONST = gc3libs.utils.Struct(
    # one can use string literal here as well, like:
    #  foo ='bar',
    codedir = os.environ['codedir'],
    openms_dir = os.environ['openms_dir'],
    # used in MRMRTNormalizer
    irt_library = os.environ['irt_library'],
    # used in MRMAnalyzer
    library = os.environ['library'],
    min_upper_edge = os.environ['min_upper_edge'],
    ini = os.environ['ini'],
    threads = 2,
    )


## auxiliary classes

def replace_suffix(filename, old_suffix, new_suffix):
    """
    Replace `old_suffix` with `new_suffix`.
    """
    # remove extension
    basename = filename[0:-len(old_suffix)]
    # re-add new one
    return (basename + new_suffix)


def make_identifier(string):
    """
    Replace all non-alphanumeric characters in `string` with underscores.
    """
    return str.join('', [(c if c.isalnum() else '_') for c in string])


class ProcessFilesInParallel(ParallelTaskCollection):
    """
    For each file in directory `directory` that matches `pattern`
    (shell glob pattern), construct a `Task` by calling `task_ctor`
    with the full pathname as unique argument.

    Run the resulting task collection in parallel and wait for all
    tasks to end.
    """

    def __init__(self, directory, pattern, task_ctor, **extra_args):
        tasks = [ ]
        for filename in os.listdir(directory):
            if not fnmatch.fnmatch(filename, pattern):
                continue
            pathname = os.path.join(directory, filename)
            tasks.append(task_ctor(pathname, **extra_args))

        ParallelTaskCollection.__init__(
            self,
            # job name
            make_identifier("Process %s files in directory %s" % (pattern, directory)),
            # list of tasks to execute
            tasks,
            # boilerplate
            **extra_args)


## define our workflow, top to bottom

class SwathWorkflow(StagedTaskCollection):
    """
    Process all the `.mzXML` files in a given directory.
    """

    def __init__(self, directory, basename, **extra_args):
        self.directory = directory
        self.basename = basename
        StagedTaskCollection.__init__(self, **extra_args)

    def stage0(self):
        """
        Run chroma extraction on `*.mzML.gz` files, and produce:
          - one `rtnorm.trafoXML` file;
          - many `._chrom.mzML` files, one per input file.

        """
        return SwathWorkflowStage0(self.directory, self.basename, pattern='*.mzML.gz')

    def stage1(self):
        """
        Run MRMAnalyzer on `*._chrom.mzML` files and produce `*_.featureXML` ones.
        """
        directory = self.tasks[0].output_dir
        # FIXME: MRMAnalyzerApp needs 3 files!!!
        return ProcessFilesInParallel(directory, self.basename, '*._chrom.mzML',
                                      MRMAnalyzerApplication)

    def stage2(self):
        """
        Run FeatureXMLToTSV on `*_.featureXML` files and produce `*_.short_format.csv` ones.
        """
        directory = self.tasks[1].output_dir
        return ProcessFilesInParallel(directory, self.basename, '*_.featureXML',
                                      FeatureXMLToTSVApplication)

    # def stage3(self):
    #     """
    #     Run MProphet on `*_.short_format.csv` files.
    #     """
    #     directory = self.tasks[2].output_dir
    #     return ProcessFilesInParallel(directory, self.basename, '*_.short_format.csv',
    #                                   MProphetApplication)


class SwathWorkflowStage0(ParallelTaskCollection):
    """
    Two sequences running in parallel:

      - chroma extraction (long job)
      - chroma extraction (short job) + file merger + MRT normalization
    """
    def __init__(self, directory, basename, pattern, **extra_args):
        self.directory = directory
        self.basename = basename
        self.pattern = basename + pattern
        self.extra_args = extra_args
        ParallelTaskCollection.__init__(
            self,
            # jobname
            make_identifier("Stage 0 of Swath workflow in directory %s processing files %s" % (directory, pattern)),
            # tasks
            [
                ProcessFilesInParallel(directory, pattern, ChromaExtractLong, **extra_args),
                ChromaExtractShortPlusNormalization(directory, pattern, **extra_args),
            ],
            # boilerplate
            **extra_args)

    def terminated(self):
        self.trafoxml_file = self.tasks[1].trafoxml_file
        # map mzML.gz files to the corresponding `._chom.mzML` file
        # (which might be in a different directory)
        self.chrom_files = { }
        for chromalong_task in self.tasks[0].tasks:
            infile = os.path.basename(chromalong_task.mzmlgz_file)
            outfile = os.path.join(chromalong_task.output_dir,
                                   replace_suffix(infile, '.mzML.gz', '._chrom.mzML'))
            self.chrom_files[infile] = outfile


class ChromaExtractLong(Application):
    application_name = 'chromatogram_extractor'
    def __init__(self, path, **extra_args):
        outfile = replace_suffix(
            os.path.basename(path, '.mzML.gz', '._chrom.mzML'))
        Application.__init__(
            self,
            executable="ChromatogramExtractor",
            arguments = [
                '-in', path,
                '-tr', CONST.library,
                '-out', outfile,
                '-is_swath',
                '-min_upper_edge_dist', CONST.min_upper_edge,
                '-threads', CONST.threads,
                ],
            inputs=[path],
            outputs=[outfile],
            **extra_args)
        self.mzmlgz_file = path
        self.outfile = outfile

    def terminated(self):
        self.chrom_file = os.path.join(self.output_dir, self.outfile)


class ChromaExtractShortPlusNormalization(StagedTaskCollection):
    """
    Run the following steps in sequence:

    1. Run chroma extraction (short version) for each file in directory matching the given pattern;
    2. Merge all produced files;
    3. Run MRMRTNormalizer on the merged file.
    """
    def __init__(self, directory, basename, pattern, **extra_args):
        self.directory = directory
        self.basename = basename
        self.pattern = basename + pattern
        self.extra_args = extra_args
        StagedTaskCollection.__init__(self, **extra_args)

    def stage0(self):
        """
        Run chroma extraction (short).
        """
        return ProcessFilesInParallel(self.directory, self.pattern, ChromaExtractShort,
                                      **self.extra_args)

    def stage1(self):
        """
        Merge all produced files.
        """
        directory = self.tasks[0].output_dir
        in_files = [ os.path.join(directory, filename)
                     for filename in os.listdir(directory)
                     if fnmatch(filename, self.pattern) ]
        outfile = self.basename + '.rtnorm.chrom.mzML'
        return Application(
            executable="FileMerger",
            arguments=["-in"] + in_files + ["-out", outfile],
            inputs=in_files,
            outputs=[outfile],
            # record this for ease of referencing from other jobs
            outfilename=outfile,
            # std options from the script
            **self.extra_args)

    def stage2(self):
        """
        Run MRMRTNormalizer on the `.rtnorm.chrom.mzML` file.
        """
        directory = self.tasks[1].output_dir
        infile = os.path.join(directory, self.tasks[1].outfilename)
        outfile = self.basename + '.rtnorm.trafoXML'
        return Application(
            executable="MRMRTNormalizer",
            arguments=['-in', infile, '-tr', cfg.irt_library, '-out', outfile],
            inputs=[infile],
            outputs=[outfile],
            # record this for ease of referencing from other jobs
            outfilename=outfile,
            # std options from the script
            **self.extra_args)

    def terminated(self):
        """
        Define the `self.trafoxml_file` attribute to the full path name
        of the `.rtnorm.trafoXML` output file.
        """
        self.trafoxml_file = os.path.join(self.tasks[2].output_dir,
                                          self.basename + '.rtnorm.trafoXML')


class ChromaExtractShort(Application):
    application_name = 'chromatogram_extractor'
    def __init__(self, path, **extra_args):
        outfile = replace_suffix(
            os.path.basename(path, '.mzML.gz', '._rtnorm.chrom.mzML'))
        Application.__init__(
            self,
            executable="ChromatogramExtractor",
            arguments = [
                '-in', path,
                '-tr', CONST.irt_library,
                '-out', outfile,
                '-is_swath',
                '-min_upper_edge_dist', CONST.min_upper_edge,
                '-threads', CONST.threads,
                ],
            inputs=[path],
            outputs=[outfile],
            **extra_args)
        self.mzmlgz_file = path
        self.outfile = outfile

    def terminated(self):
        self.chrom_file = os.path.join(self.output_dir, self.outfile)


class MRMAnalyzerApplication(Application):
    application_name = 'mrmanalyzer'
    def __init__(self, mzmlgz_file, trafoxml_file, chrom_file, **extra_args):
        assert mzmlgz_filename.endswith('.mzML.gz')
        assert trafoxml_file.endswith('rtnorm.trafoXML')
        outfile = replace_suffix(mzmlgz_file, '.mzML.gz', '_.featureXML')
        Application.__init__(
            self,
            executable="MRMAnalyzer",
            arguments=[
                '-in', chrom_file,
                '-swath_files', mzmlgz_file,
                '-tr', CONST.library,
                '-out', outfile,
                '-min_upper_edge_dist', CONST.min_upper_edge,
                '-ini', CONST.ini,
                '-rt_norm', trafoxml_file,
                '-threads', CONST.threads,
                ],
            inputs=[mzmlgz_file, trafoxml_file, chrom_file],
            outputs=[outfile],
            **extra_args)


class FeatureXMLToTSVApplication(Application):
    application_name = 'featurexml_to_tsv'
    def __init__(self, path):
        assert path.endswith('_.featureXML')
        outfile = replace_suffix(mzmlgz_file, '_.featureXML',
                                              '_.short_format.csv')
        Application.__init__(
            self,
            executable="FeatureXMLToTSV",
            arguments=[
                '-tr', CONST.library,
                '-in', path,
                '-out', outfile,
                '-short_format',
                '-threads', CONST.threads,
                ],
            inputs=[path],
            outputs=[outfile],
            **extra_args)


# class MProphetApplication(Application):
#     def __init__(self, path):
#         Application.__init__(
#             self,
#             executable="MProphet",
#             arguments=[],
#             inputs=[path],
#             outputs=[outfile],
#             **extra_args)
