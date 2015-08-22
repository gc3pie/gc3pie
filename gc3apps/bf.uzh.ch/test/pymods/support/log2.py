import os
import sys
import logging
from optparse import OptionParser

def initialize_logging(options):
    """ Log information based upon users options"""

    logger = logging.getLogger('project')
    formatter = logging.Formatter('%(asctime)s %(levelname)s\t%(message)s')
    level = logging.__dict__.get(options.loglevel.upper(),logging.DEBUG)
    logger.setLevel(level)

    # Output logging information to screen
    if not options.quiet:
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

    # Output logging information to file
    logfile = os.path.join(options.logdir, "project.log")
    if options.clean and os.path.isfile(logfile):
        os.remove(logfile)
    hdlr2 = logging.FileHandler(logfile)
    hdlr2.setFormatter(formatter)
    logger.addHandler(hdlr2)

    return logger

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # Setup command line options
    parser = OptionParser("usage: %prog [options]")
    parser.add_option("-l", "--logdir", dest="logdir", default=".", help="log DIRECTORY (default ./)")
    parser.add_option("-v", "--loglevel", dest="loglevel", default="debug", help="logging level (debug, info, error)")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="do not log to console")
    parser.add_option("-c", "--clean", dest="clean", action="store_true", default=False, help="remove old log file")

    # Process command line options
    (options, args) = parser.parse_args(argv)

    # Setup logger format and output locations
    logger = initialize_logging(options)

    # Examples
    logger.error("This is an error message.")
    logger.info("This is an info message.")
    logger.debug("This is a debug message.")

if __name__ == "__main__":
    sys.exit(main())