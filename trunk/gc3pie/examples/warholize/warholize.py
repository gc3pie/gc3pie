#! /usr/bin/env python
#
"""
`Warholize` is an example script to show how a generic workflow can be
made in GC3Pie.

The script will take a picture and will produce a new picture similar
to the famous Warhol picture series.

"""
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
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

### Common modules

import os
import tempfile
import itertools
import random
import re
import math

### GC3 imports

# Main module
import gc3libs

# Run class is used to return the current state of the
# application/workflow
from gc3libs import Run

# This classes are the main workflows we will use: sequential and
# parallel
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection

# To create the script
from gc3libs.cmdline import SessionBasedScript

# An handy function to copy files
from gc3libs.utils import copyfile


class ApplicationWithCachedResults(gc3libs.Application):
    """
    Just like `gc3libs.Application`, but do not run at all
    if the expected result is already present on the filesystem.
    """
    def __init__(self, executable, arguments, inputs, outputs, **kw):
        gc3libs.Application.__init__(self, executable, arguments, inputs, outputs, **kw)
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


class GrayScaleConvertApplication(ApplicationWithCachedResults):
    def __init__(self, input_image, grayscaled_image, output_dir, warhol_dir, resize=None):
        self.warhol_dir = warhol_dir
        self.grayscaled_image = grayscaled_image

        arguments = [
            input_image,
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
            executable = 'convert',
            arguments = arguments + [grayscaled_image],
            inputs = [input_image],
            outputs = [grayscaled_image, 'stderr.txt', 'stdout.txt'],
            output_dir = output_dir,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

    def terminated(self):
        """Move grayscale image to the main output dir"""
        copyfile(
            os.path.join(self.output_dir, self.grayscaled_image),
            self.warhol_dir)

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
            executable = "convert",
            arguments = [
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

class ApplyLutApplication(ApplicationWithCachedResults):
    """Apply the LUT computed by `CreateLutApplication`:class: to
    `image_file`"""

    def __init__(self, input_image, lutfile, output_file, output_dir, working_dir):

        gc3libs.log.info("Applying lut file %s to %s" % (lutfile, input_image))
        self.working_dir = working_dir
        self.output_file = output_file

        ApplicationWithCachedResults.__init__(
            self,
            executable = "convert",
            arguments = [
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
        copyfile(
            os.path.join(self.output_dir, self.output_file),
            self.working_dir)


class TricolorizeImage(SequentialTaskCollection):

    def __init__(self, grayscaled_image, output_dir, output_file,
                 colors, warhol_dir, grid=None):
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

        SequentialTaskCollection.__init__(self, self.jobname, self.tasks, grid)

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


class TricolorizeMultipleImages(ParallelTaskCollection):
    colors = ['yellow', 'blue', 'red', 'pink', 'orchid',
              'indigo', 'navy', 'turquoise1', 'SeaGreen', 'gold',
              'orange', 'magenta']

    def __init__(self, grayscaled_image, copies, ncolors, output_dir, grid=None):
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
                self.warhol_dir,
                grid=grid))

        ParallelTaskCollection.__init__(self, self.jobname, self.tasks, grid)


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
            executable = 'montage',
            arguments = input_filenames + [
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
        copyfile(os.path.join(self.output_dir,
                              self.output_file),
                 self.input_dir)


class WarholizeWorkflow(SequentialTaskCollection):
    """Main workflow.

    The sequence of tasks is basically:

    1) create a grayscaled version of the input image

    2) run a `ParallelTaskCollection`:class: which will produce
    `copies` different colorized versions of the grayscale image

    3) merge together the `copies` images in order to create one
    single `warholized` image.
    """

    def __init__(self, input_image,  copies, ncolors,
                 grid=None, size=None, **kw):
        """XXX do we need input_image and output_image? I guess so?"""
        self.input_image = input_image
        self.output_image = "warhol_%s" % input_image
        self.resize = False

        gc3libs.log.info(
            "Producing a warholized version of input file %s "
            "and store it in %s" % (input_image, self.output_image))


        if size:
            x, y = size.split('x', 2)
            rows = math.sqrt(copies)
            self.resize = "%dx%d" % (int(x) / rows, int(y) / rows)
        # XXX: it is complaining about absolute path???
        # InvalidArgument: Remote paths not allowed to be absolute
        self.output_dir = os.path.relpath(kw.get('output_dir'))

        self.ncolors = ncolors
        self.copies = copies
        # Check that copies is a perfect square
        if math.sqrt(self.copies) != int(math.sqrt(self.copies)):
            raise gc3libs.exceptions.InvalidArgument(
                "`copies` argument must be a perfect square.")

        self.jobname = kw.get('jobname', 'WarholizedWorkflow')

        # XXX needs to create a temporary directory?

        # These attributes are temporary files
        self.grayscaled_image = "grayscaled_%s" % self.input_image

        self.tasks = [
            GrayScaleConvertApplication(
                self.input_image, self.grayscaled_image, self.output_dir,
                self.output_dir, resize=self.resize),
            ]

        SequentialTaskCollection.__init__(
            self, self.jobname, self.tasks, grid=grid)

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


class WarholizeScript(SessionBasedScript):
    """Demo script to create a `Warholized` version of an image."""
    version='1.0'
    
    def setup_options(self):
        self.add_param('--copies', default=4, type=int,
                       help="Number of copyes (Default:4). It has to be a perfect square!")
        self.add_param('--size', default=None,
                       help="Resize the original image."
                       "Please note that the resulting image will be N times the size "
                       "specified here, where N is the argument of --copies.")
        self.add_param('-n', '--num-colors', default=3, type=int,
                       help="Number of colors to use. Default: 3")

    def new_tasks(self, extra):
        extra
        if self.params.size:
            extra['size'] = self.params.size
        gc3libs.log.info("Creating main sequential task")
        for (i, input_file) in enumerate(self.params.args):
            kw = extra.copy()
            kw['output_dir'] = 'Warholized.%s' % os.path.basename(input_file)
            yield ("Warholize.%d" % i,
                   WarholizeWorkflow,
                   [input_file,
                    self.params.copies,
                    self.params.num_colors],
                   kw)


# run script
if __name__ == '__main__':
    WarholizeScript().run()
