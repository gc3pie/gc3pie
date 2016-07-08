from gc3libs import Application

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
      stderr="stderr.txt")
