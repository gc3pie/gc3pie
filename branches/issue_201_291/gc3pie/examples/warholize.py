#! /usr/bin/env python
#
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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


# GC3Pie tutorial - Warholize
# ===========================
#
# In this tutorial we will show you how to use GC3Pie libraries in order
# to build a command line script which will run a complex workflow with
# both parallel and sequential tasks.
#
# The tutorial itself contains the complete source code of the
# application (cfr. `Literate Programming`_ on Wikipedia), so that you
# will be able to produce a working ``warholize.py`` script and
# test/modify it you by downloading **pylit** from the `PyLit
# Homepage`_, and running the following command on the
# ``gc3pie/docs/gc3libs/tutorial/warholize.txt`` file, from whitin the
# svn tree of GC3Pie::

# $    ./pylit warholize.txt warholize.py
#
#
# Introduction
# ------------
#
# `Warholize` is a GC3Pie demo application to produce, from a generic
# image picture, a new picture like the famous Warhol's work:
# `Marylin`_. The script uses the powerful `ImageMagick`_ set of tools
# (at least version 6.3.5-7). This tutorial will assume that both
# `ImageMagick` and `GC3Pie` are already installed and configured.
#
# In order to produce a similar image we have to do a series of
# transformations on the picture:
#
# 1) convert the original image to grayscale.
#
# 2) `colorize` the grayscale image using three different colors each
#    time, based on the gray levels. We may, for instance, make all
#    pixels with luminosity between 0-33% in red, pixels
#    between 34-66% in yellow and pixels between 67% and 100% in green.
#
#    To do that, we first have to:
#
#    a) create a `Color Lookup Table` (LUT) using a combination of three
#       randomly chosen colors
#
#    b) apply the LUT to the grayscale image
#
# 3) Finally, we can merge together all the colorized images and produce
#    our `warholized` image.
#
# Clearly, step 2) depends on the step 1), and 3) depends on 2), so we
# basically have a sequence of tasks, but since step 2) need to create
# `N` different independent images, we can parallelize this step. In
# ASCII art, the workflow will looks like::

# .                        .-------------------.
# .                        | create grayscaled |
# .                        |       image       |
# .                        `-------------------'
# .                        /      |      \      \
# .                       /       |       \      \
# .                      /        |        \      \
# .        .------------.  .------------.       .------------.
# .        | create LUT |  | create LUT |  ...  | create LUT |
# .        `------------'  `------------'       `------------'
# .                   |           |         |      |
# .        .------------.  .------------.       .------------.
# .        | apply  LUT |  |  apply LUT |  ...  |  apply LUT |
# .        `------------'  `------------'       `------------'
# .                     \         |        /      /
# .                      \        |       /      /
# .                       \       |      /      /
# .                        .-------------------.
# .                        | merge `colorized` |
# .                        |       images      |
# .                        `-------------------'
#
# From top to bottom
# ------------------
#
# We will write our script starting from the top and will descend to the
# bottom, from the command line script, to the workflow and finally to
# the single execution units which compose the application.
#
#
# The script
# ----------
#
# The `SessionBasedScript` class in the `gc3libs.cmdline` module is used
# to create a generic script. It already have all what is needed to read
# gc3pie configuration files, manage resources etc. The only thing
# missing is, well, your application!
#
# Let's start importing it, among with the main `gc3libs` module::

import gc3libs
from gc3libs.cmdline import SessionBasedScript
import os

# and then, we create a class which inherits from it (in GC3Pie, most of
# the customization are done by inheriting from a more generic class)::

class WarholizeScript(SessionBasedScript):
    """
    Demo script to create a `Warholized` version of an image.
    """
    version='1.0'

# Please note that you must either write a small docstring for that
# class, or add a `description` attribute.
#
# The way we want to use our script is straightforward::

# $  warholize.py inputfile [inputfiles ...]
#
# and this will create a directory ``Warholized.<inputfile>`` in which
# there will be a file called ``warhol_<inputfile>`` containing the
# desired warholized image (and a lot of temporary files, at least for now).
#
# But we may want to add some optional argument to the script, in order
# to decide how many colorized pictures the warholized image will be
# made of, or if we want to resize the image. `SessionBasedScript` uses
# the `PyCLI`_ module which is in turn a wrapper around standard
# `argparse` (or `optparse`) python modules. To customize the script you
# can define a `setup_options` method and put in there some calls to
# `SessionBasedScript.add_param()`, which is inherited from
# `cli.app.CommandLineApp`::

    def setup_options(self):
        self.add_param('--copies', default=4, type=int,
                       help="Number of copyes (Default:4). It has to be a perfect square!")
        self.add_param('--size', default=None,
                       help="Size of the resulting image. "
                       "Please note that the resulting image will be a bit "
                       "bigger than this value because of the tile. For "
                       "instance, if you run with --size 480x480 and N "
                       "copies, the resulting image will be "
                       "480+5*2*(sqrt(N)) = 510 pixels wide."
                       "")
        self.add_param('-n', '--num-colors', default=3, type=int,
                       help="Number of colors to use. Default: 3")


# The *heart* of the script is, however, the `new_tasks` method, which
# will be called to create the initial tasks of the scripts. In our
# case it will be something like::

    def new_tasks(self, extra):
        extra
        if self.params.size:
            extra['size'] = self.params.size
        gc3libs.log.info("Creating main sequential task")
        for (i, input_file) in enumerate(self.params.args):
            extra_args = extra.copy()
            extra_args['output_dir'] = 'Warholized.%s' % os.path.basename(input_file)
            yield ("Warholize.%d" % i,
                   WarholizeWorkflow,
                   [input_file,
                    self.params.copies,
                    self.params.num_colors],
                   extra_args)

# `new_tasks` is used as a *generator* (but it could return a list as
# well). Each *yielded* object is a tuple which rapresents a generic
# Task. In GC3Pie, a task is either a single task or a complex workflow,
# and rapresents an *execution unit*. In our case we create a
# `WarholizeWorkflow` task which is the workflow described before. We
# don't create an instance of the task from whitin `new_tasks`, but we
# pass all the arguments needed. In the order:
#
#   * The job name (used to identify the task inside the session)
#
#   * the class object (not the instance!)
#
#   * arguments to be passed to the constructor of the class
#
#   * a dictionary containing the keyword arguments to be passed to the
#     constructor of the class
#
# In our case we yield a different `WarholizeWorkflow` task for each
# input file. These tasks will then run in parallel.
#
#
#
# The workflows
# -------------
#
# Main sequential workflow
# ++++++++++++++++++++++++
#
# The module `gc3libs.workflow` contains two main objects,
# `SequentialTaskCollection` and `ParallelTaskCollection` which we will
# use to create our workflow. The first one, `WarholizeWorkflow`, is a
# sequential one, so::

from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import math
from gc3libs import Run

class WarholizeWorkflow(SequentialTaskCollection):
    """
    Main workflow.
    """

    def __init__(self, input_image,  copies, ncolors,
                 size=None, **extra_args):
        """XXX do we need input_image and output_image? I guess so?"""
        self.input_image = input_image
        self.output_image = "warhol_%s" % os.path.basename(input_image)
        self.resize = False

        gc3libs.log.info(
            "Producing a warholized version of input file %s "
            "and store it in %s" % (input_image, self.output_image))


        if size:
            x, y = size.split('x', 2)
            rows = math.sqrt(copies)
            self.resize = "%dx%d" % (int(x) / rows, int(y) / rows)

        self.output_dir = os.path.relpath(extra_args.get('output_dir'))

        self.ncolors = ncolors
        self.copies = copies

        # Check that copies is a perfect square
        if math.sqrt(self.copies) != int(math.sqrt(self.copies)):
            raise gc3libs.exceptions.InvalidArgument(
                "`copies` argument must be a perfect square.")

        self.jobname = extra_args.get('jobname', 'WarholizedWorkflow')

        self.grayscaled_image = "grayscaled_%s" % os.path.basename(self.input_image)

# This is just parsing of the arguments. The last lines, instead,
# create the initial tasks that will be submitted. By now, we can submit
# only the first one, `GrayScaleConvertApplication`, which will produce
# a grayscale image from the input file::

        self.tasks = [
            GrayScaleConvertApplication(
                self.input_image, self.grayscaled_image, self.output_dir,
                self.output_dir, resize=self.resize),
            ]

        SequentialTaskCollection.__init__(self, self.tasks)

# Finally, we to call the parent's constructor.
#
# This will create the initial task list, but we have to run also step 2
# and 3. This is done by creating a `next` method. This method will be
# called after all the tasks in `self.tasks` are finished. We cannot
# create all the jobs at once because we don't have all the needed input
# files yet.
#
# The `next` method will look like::

    def next(self, iteration):
        last = self.tasks[-1]

        if isinstance(last, GrayScaleConvertApplication):
            self.add(TricolorizeMultipleImages(
                os.path.join(self.output_dir, self.grayscaled_image),
                self.copies, self.ncolors,
                self.output_dir))
            return Run.State.RUNNING
        elif isinstance(last, TricolorizeMultipleImages):
            self.add(MergeImagesApplication(
                os.path.join(self.output_dir, self.grayscaled_image),
                last.warhol_dir,
                self.output_image))
            return Run.State.RUNNING
        else:
            self.execution.returncode = last.execution.returncode
            return Run.State.TERMINATED

# At each iteration, we call `self.add()` to add an instance of a
# task-like class (`gc3libs.Application`,
# `gc3libs.workflow.ParallelTaskCollection` or
# `gc3libs.workflow.SequentialTaskCollection`, in our case) to complete the
# next step, and we return the current state, which will be
# `gc3libs.Run.State.RUNNING` unless we have finished the computation.
#
#
# Step one: convert to grayscale
# ++++++++++++++++++++++++++++++
#
# `GrayScaleConvertApplication` is the application responsible to
# convert to grayscale the input image, and resize it if needed. To
# create an application we usually inherit from the
# `gc3libs.Application` class, but in our case we want each application
# *not to produce output if it already exists*, so first of all we
# create a generic *cached* application which wraps
# `gc3libs.Application`::

class ApplicationWithCachedResults(gc3libs.Application):
    """
    Just like `gc3libs.Application`, but do not run at all
    if the expected result is already present on the filesystem.
    """
    def __init__(self, arguments, inputs, outputs, **extra_args):
        gc3libs.Application.__init__(self, arguments, inputs, outputs, **extra_args)
        # check if all the output files are already available

        all_outputs_available = True
        for output in self.outputs.values():
            if not os.path.exists(
                os.path.join(self.output_dir, output.path)):
                all_outputs_available = False
        if all_outputs_available:
            # skip execution altogether
            gc3libs.log.info("Skipping execution since all output files are availables")
            self.execution.state = Run.State.TERMINATED

# and then we create our GrayScaleConvertApplication as::

# An useful function to copy files
from gc3libs.utils import copyfile

class GrayScaleConvertApplication(ApplicationWithCachedResults):
    def __init__(self, input_image, grayscaled_image, output_dir, warhol_dir, resize=None):
        self.warhol_dir = warhol_dir
        self.grayscaled_image = grayscaled_image

        arguments = [
            'convert',
            os.path.basename(input_image),
            '-colorspace',
            'gray',
            ]
        if resize:
            arguments.extend(['-geometry', resize])

        gc3libs.log.info(
            "Craeting  GrayScale convert application from file %s"
            "to file %s" % (input_image, grayscaled_image))

        ApplicationWithCachedResults.__init__(
            self,
            arguments = arguments + [grayscaled_image],
            inputs = [input_image],
            outputs = [grayscaled_image, 'stderr.txt', 'stdout.txt'],
            output_dir = output_dir,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

# Creating a `gc3libs.Application` is straigthforward: you just
# call the constructor with the executable, the arguments, and the
# input/output files you will need.
#
# If you don't specify the ``output_dir`` directory, gc3pie libraries will
# create one starting from the job name. It is quite important, then, to
# generate unique jobname for your applications in order to avoid
# conflits. If the output directory exists already, the old one will be
# renamed.
#
# To do any kind of post processing you can define a `terminate` method
# for your application. It will be called after your application will
# terminate. In our case we want to copy the gray scale version of the
# image to the `warhol_dir`, so that it will be easily reachable by the
# other applications::

    def terminated(self):
        """Move grayscale image to the main output dir"""
        try:
            copyfile(
                os.path.join(self.output_dir, self.grayscaled_image),
                self.warhol_dir)
        except:
            gc3libs.log.warning("Ignoring error copying file %s to %s" % (
                os.path.join(self.output_dir, self.grayscaled_image),
                self.warhol_dir))



# Step two: parallel workflow to create colorized images
# ------------------------------------------------------
#
# The `TricolorizeMultipleImages` is responsible to create multiple
# versions of the grayscale image with different coloration. It does it
# by running multiple instance of `TricolorizeImage` with different
# color arguments. Since we want to run the various colorization in
# parallel, it inherits from `gc3libs.workflow.ParallelTaskCollection` class::

import itertools
import random

class TricolorizeMultipleImages(ParallelTaskCollection):
    colors = ['yellow', 'blue', 'red',
              'navy', 'turquoise1', 'SeaGreen', 'gold',
              'orange', 'magenta']

    def __init__(self, grayscaled_image, copies, ncolors, output_dir):
        gc3libs.log.info(
            "TricolorizeMultipleImages for %d copies run" % copies)
        self.jobname = "Warholizer_Parallel"
        self.ncolors = ncolors
        ### XXX Why I have to use basename???
        self.output_dir = os.path.join(
            os.path.basename(output_dir), 'tricolorize')
        self.warhol_dir = output_dir

        # Compute a unique sequence of random combination of
        # colors. Please note that we can have a maximum of N!/3! if N
        # is len(colors)
        assert copies <= math.factorial(len(self.colors)) / math.factorial(ncolors)

        combinations = [i for i in itertools.combinations(self.colors, ncolors)]
        combinations = random.sample(combinations, copies)

        # Create all the single tasks
        self.tasks = []
        for i, colors in enumerate(combinations):
            self.tasks.append(TricolorizeImage(
                os.path.relpath(grayscaled_image),
                "%s.%d" % (self.output_dir, i),
                "%s.%d" % (grayscaled_image, i),
                colors,
                self.warhol_dir))

        ParallelTaskCollection.__init__(self, self.tasks)

# The main loop will fill the `self.tasks` list with various
# `TricolorizedImage`, each one with an unique combination of three
# colors to use to generate the colorized image.
#
# The `TricolorizedImage` class is indeed a `SequentialTaskCollection`,
# since it has to generate the LUT first, and then apply it to the
# grayscale image. Again, the constructor of the class will add the
# first job (`CreateLutApplication`), and the `next` method will take
# care of running the `ApplyLutApplication` application on the output of
# the first job::


class TricolorizeImage(SequentialTaskCollection):
    """
    Sequential workflow to produce a `tricolorized` version of a
    grayscale image
    """
    def __init__(self, grayscaled_image, output_dir, output_file,
                 colors, warhol_dir):
        self.grayscaled_image = grayscaled_image
        self.output_dir = output_dir
        self.warhol_dir = warhol_dir
        self.jobname = 'TricolorizeImage'
        self.output_file = output_file

        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        gc3libs.log.info(
            "Tricolorize image %s to %s" % (
                self.grayscaled_image, self.output_file))

        self.tasks = [
            CreateLutApplication(
                self.grayscaled_image,
                "%s.miff" % self.grayscaled_image,
                self.output_dir,
                colors, self.warhol_dir),
            ]

        SequentialTaskCollection.__init__(self, self.tasks)

    def next(self, iteration):
        last = self.tasks[-1]
        if isinstance(last, CreateLutApplication):
            self.add(ApplyLutApplication(
                self.grayscaled_image,
                os.path.join(last.output_dir, last.lutfile),
                os.path.basename(self.output_file),
                self.output_dir, self.warhol_dir))
            return Run.State.RUNNING
        else:
            self.execution.returncode = last.execution.returncode
            return Run.State.TERMINATED

# The `CreateLutApplication` is again an application which inherits from
# `ApplicationWithCachedResults` because we don't want to compute over
# and over the same LUT, so::

class CreateLutApplication(ApplicationWithCachedResults):
    """Create the LUT for the image using 3 colors picked randomly
    from CreateLutApplication.colors"""

    def __init__(self, input_image, output_file, output_dir, colors, working_dir):
        self.lutfile = os.path.basename(output_file)
        self.working_dir = working_dir
        gc3libs.log.info("Creating lut file %s from %s using "
                         "colors: %s" % (
            self.lutfile, input_image, str.join(", ", colors)))
        ApplicationWithCachedResults.__init__(
            self,
            arguments = [
                'convert',
                '-size',
                '1x1'] + [
                "xc:%s" % color for color in colors] + [
                '+append',
                '-resize',
                '256x1!',
                self.lutfile,
                ],
            inputs = [input_image],
            outputs = [self.lutfile, 'stdout.txt', 'stderr.txt'],
            output_dir = output_dir + '.createlut',
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

# And the `ApplyLutApplication` as well::

class ApplyLutApplication(ApplicationWithCachedResults):
    """Apply the LUT computed by `CreateLutApplication` to
    `image_file`"""

    def __init__(self, input_image, lutfile, output_file, output_dir, working_dir):

        gc3libs.log.info("Applying lut file %s to %s" % (lutfile, input_image))
        self.working_dir = working_dir
        self.output_file = output_file

        ApplicationWithCachedResults.__init__(
            self,
            arguments = [
                'convert',
                os.path.basename(input_image),
                os.path.basename(lutfile),
                '-clut',
                output_file,
                ],
            inputs = [input_image, lutfile],
            outputs = [output_file, 'stdout.txt', 'stderr.txt'],
            output_dir = output_dir + '.applylut',
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

    def terminated(self):
        """Copy colorized image to the output dir"""
        try:
            copyfile(
                os.path.join(self.output_dir, self.output_file),
                self.working_dir)
        except:
            gc3libs.log.warning("Ignoring error copying file %s to %s." % (
                os.path.join(self.output_dir, self.output_file),
                self.working_dir))

# which will copy the colorized image file in the top level directory,
# so that it will be easier for the last application to find all the
# needed files.
#
#
# Step three: merge all them together
# +++++++++++++++++++++++++++++++++++
#
# At this point we will have in the main output directory a bunch of
# files named after ``grayscaled_<input_image>.N`` with N a sequential
# integer and ``<input_image>`` the name of the original image. The last
# application, `MergeImagesApplication`, will produce a
# ``warhol_<input_image>`` imagme merging all of them. Now it should be
# easy to write such application::

import re

class MergeImagesApplication(ApplicationWithCachedResults):
    def __init__(self, grayscaled_image, input_dir, output_file):
        ifile_regexp = re.compile(
            "%s.[0-9]+" % os.path.basename(grayscaled_image))
        input_files = [
            os.path.join(input_dir, fname) for fname in os.listdir(input_dir)
            if ifile_regexp.match(fname)]
        input_filenames = [os.path.basename(i) for i in input_files]
        gc3libs.log.info("MergeImages initialized")
        self.input_dir = input_dir
        self.output_file = output_file

        tile = math.sqrt(len(input_files))
        if tile != int(tile):
            gc3libs.log.error(
                "We would expect to have a perfect square"
                "of images to merge, but we have %d instead" % len(input_files))
            raise gc3libs.exceptions.InvalidArgument(
                "We would expect to have a perfect square of images to merge, but we have %d instead" % len(input_files))

        ApplicationWithCachedResults.__init__(
            self,
            arguments = ['montage'] + input_filenames + [
                '-tile',
                '%dx%d' % (tile, tile),
                '-geometry',
                '+5+5',
                '-background',
                'white',
                output_file,
                ],
            inputs = input_files,
            outputs = [output_file, 'stderr.txt', 'stdout.txt'],
            output_dir = os.path.join(input_dir, 'output'),
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

    def terminated(self):
        """Copy output file to main directory"""
        try:
            copyfile(os.path.join(self.output_dir,
                                  self.output_file),
                     self.input_dir)
        except:
            gc3libs.log.warning("Ignoring error copying file %s to %s" % (
                os.path.join(self.output_dir,
                             self.output_file),
                self.input_dir))


# Making the script executable
# ----------------------------
#
# Finally, in order to make the script *executable*, we add the
# following lines to the end of the file. The `WarholizeScritp().run()`
# call will be executed only when the file is run as a script, and will
# do all the magic related to argument parsing, creating the session
# etc...::

if __name__ == '__main__':
    import warholize
    warholize.WarholizeScript().run()

# Please note that the ``import warholize`` statement is important to
# address `issue 95`_ and make the gc3pie scripts work with your current
# session (`gstat`, `ginfo`...)
#
# Testing
# -------
#
# To test this script I would suggest to use the famous `Lena` picture,
# which can be found in the `miscelaneous` section of the `Signal and
# Image Processing Institute`_ page. Download the image, rename it as
# ``lena.tiff`` and run the following command::

# $    ./warholize.py -C 1 lena.tiff --copies 9
#
# (add ``-r localhost`` if your gc3pie.conf script support it and you
# want to test it locally).
#
# After completion a file ``Warholized.lena.tiff/warhol_lena.tiff``
# will be created.
#
#
# .. Links
#
# .. _`Literate Programming`: http://en.wikipedia.org/wiki/Literate_programming
#
# .. _`PyLit Homepage`: http://pylit.berlios.de/index.html
#
# .. _`Marylin`: http://artobserved.com/artists/andy-warhol/
#
# .. _`ImageMagick`: http://www.imagemagick.org/
#
# .. _`PyCLI`: http://packages.python.org/pyCLI/
#
# .. _`Signal and Image Processing Institute`: http://sipi.usc.edu/database/?volume=misc
#
# .. _`issue 95`: http://code.google.com/p/gc3pie/issues/detail?id=95
