import os
from os.path import basename

from gc3libs import Application
from gc3libs.quantity import GB


class GrayscaleApp(Application):
    """Convert an image file to grayscale."""
    def __init__(self, img):
        inp = basename(img)
        out = "gray-" + inp
        Application.__init__(
            self,
            arguments=[
                "convert", inp, "-colorspace", "gray", out],
            inputs=[img],
            outputs=[out],
            output_dir="grayscale.d",
            stdout="stdout.txt",
            stderr="stderr.txt",
            # this is needed to circumvent GC3Pie issue #559, see
            # <https://github.com/uzh/gc3pie/issues/559>
            requested_memory=1*GB)
