#import logging
#log = logging.getLogger("htpie")
from htpie.lib.utils import *
if not get_logger():
    configure_logger(1000)
log = get_logger()
