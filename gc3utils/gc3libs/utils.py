#! /usr/bin/env python
"""
Generic Python programming utility functions.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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


import gc3libs
import os
import os.path
import re
import shelve
import sys
import time

import warnings
warnings.simplefilter("ignore")

# import for send_mail
import tarfile
import smtplib
import os
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

from Exceptions import *
from arclib import *
from lockfile import FileLock
    

# ================================================================
#
#                     Generic functions
#
# ================================================================


class defaultdict(dict):
    """
    A backport of `defaultdict` to Python 2.4
    See http://docs.python.org/library/collections.html
    """
    def __new__(cls, default_factory=None):
        return dict.__new__(cls)
    def __init__(self, default_factory):
        self.default_factory = default_factory
    def __missing__(self, key):
        try:
            return self.default_factory()
        except:
            raise KeyError("Key '%s' not in dictionary" % key)
    def __getitem__(self, key):
        if not dict.__contains__(self, key):
            dict.__setitem__(self, key, self.__missing__(key))
        return dict.__getitem__(self, key)


def first(seq):
    """
    Return the first element of sequence or iterator `seq`.
    Raise `TypeError` if the argument does not implement
    either of the two interfaces.

    Examples::

      >>> s = [0, 1, 2]
      >>> first(s)
      0

      >>> s = {'a':1, 'b':2, 'c':3}
      >>> first(sorted(s.keys()))
      'a'
    """
    try: # try iterator interface
        return seq.next()
    except AttributeError:
        pass
    try: # seq is no iterator, try indexed lookup
        return seq[0]
    except IndexError:
        pass
    raise TypeError("Argument to `first()` method needs to be iterator or sequence.")


class Struct(dict):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.

    Examples::

      >>> a = Struct()
      >>> a['x'] = 1
      >>> a.x
      1
      >>> a.y = 2
      >>> a['y']
      2

    Values can also be initially set by specifying them as keyword
    arguments to the constructor::

      >>> a = Struct(z=3)
      >>> a['z']
      3
      >>> a.z
      3
    """
    def __init__(self, initializer=None, **kw):
        if initializer is None:
            dict.__init__(self, **kw)
        else:
            dict.__init__(self, initializer, **kw)
    def __setattr__(self, key, val):
        self[key] = val
    def __getattr__(self, key):
        if self.has_key(key):
            return self[key]
        else:
            try:
                raise AttributeError("No attribute '%s' on object %s" % (key, self))
            except RuntimeError:
                raise AttributeError("No attribute '%s' on %s", safe_repr(self))
    def __hasattr__(self, key):
        return self.has_key(key)
    def __getstate__(self):
        return False


def progressive_number():
    """
    Return a positive integer, whose value is guaranteed to
    be monotonically increasing across different invocations
    of this function, and also across separate instances of the
    calling program.

    Example::

      >>> n = progressive_number()
      >>> m = progressive_number()
      >>> m > n
      True

    After every invocation of this function, the returned number
    is stored into the file ``~/.gc3/next_id.txt``.

    *Note:* as file-level locking is used to serialize access to the
    counter file, this function may block (default timeout: 30
    seconds) while trying to acquire the lock, or raise an exception
    if this fails.
    """
    # FIXME: should use global config value for directory
    id_filename = os.path.expanduser("~/.gc3/next_id.txt")
    # ``FileLock`` requires that the to-be-locked file exists; if it
    # does not, we create an empty one (and avoid overwriting any
    # content, in case another process is also writing to it).  There
    # is thus no race condition here, as we attempt to lock the file
    # anyway, and this will stop concurrent processes.
    if not os.path.exists(id_filename):
        open(id_filename, "a").close()
    lock = FileLock(id_filename, threaded=False) 
    lock.acquire(timeout=30) # XXX: can raise 'LockTimeout'
    id_file = open(id_filename, 'r+')
    id = int(id_file.read(8) or "0", 16)
    id +=1 
    id_file.seek(0)
    id_file.write("%08x -- DO NOT REMOVE OR ALTER THIS FILE: it is used internally by the gc3libs\n" % id)
    id_file.close()
    lock.release()
    return id


def defproperty(fn):
    """
    Decorator to define properties with a simplified syntax in Python 2.4.
    See http://code.activestate.com/recipes/410698-property-decorator-for-python-24/#c6
    for details and examples.
    """
    return property(**fn())


def dirname(pathname):
    """
    Same as `os.path.dirname` but return `.` in case of path names with no directory component.
    """
    dirname = os.path.dirname(pathname)
    if not dirname:
        dirname = '.'
    # FIXME: figure out if this is a desirable outcome.  i.e. do we want dirname to be empty, or do a pwd and find out what the current dir is, or keep the "./".  I suppose this could make a difference to some of the behavior of the scripts, such as copying files around and such.
    return dirname


def check_jobdir(jobdir):
    """
    Perform various checks on the jobdir.
    Right now we just make sure it exists.  In the future it could include checks for:

    - are the files inside valid
    - etc.
    """

    if os.path.isdir(jobdir):
        return True
    else:
        return False


def deploy_configuration_file(filename, template_filename=None):
    """
    Ensure that configuration file `filename` exists; possibly
    copying it from the specified `template_filename`.

    Return `True` if a file with the specified name exists in the 
    configuration directory.  If not, try to copy the template file
    over and then return `False`; in case the copy operations fails, 
    a `NoConfigurationFile` exception is raised.

    If parameter `filename` is not an absolute path, it is interpreted
    as relative to `gc3libs.Default.RCDIR`; if `template_filename` is
    `None`, then it is assumed to be the same as `filename`.
    """
    if template_filename is None:
        template_filename = os.path.basename(filename)
    if not os.path.isabs(filename):
        filename = os.path.join(Default.RCDIR, filename)
    if os.path.exists(filename):
        return True
    else:
        try:
            # copy sample config file 
            if not os.path.exists(dirname(filename)):
                os.makedirs(dirname(filename))
            from pkg_resources import Requirement, resource_filename
            sample_config = resource_filename(Requirement.parse("gc3libs"), 
                                              "gc3libs/etc/" + template_filename)
            import shutil
            shutil.copyfile(sample_config, filename)
            return False
        except IOError, x:
            gc3libs.log.critical("CRITICAL ERROR: Failed copying configuration file: %s" % x)
            raise NoConfigurationFile("No configuration file '%s' was found, and an attempt to create it failed. Aborting." % filename)
        except ImportError:
            raise NoConfigurationFile("No configuration file '%s' was found. Aborting." % filename)


def from_template(template, **kw):
    """
    Return the contents of `template`, substituting all occurrences
    of Python formatting directives '%(key)s' with the corresponding values 
    taken from dictionary `kw`.

    If `template` is an object providing a `read()` method, that is
    used to gather the template contents; else, if a file named
    `template` exists, the template contents are read from it;
    otherwise, `template` is treated like a string providing the
    template contents itself.
    """
    if hasattr(template, 'read') and callable(template.read):
        template_contents = template.read()
    elif os.path.exists(template):
        template_file = file(template, 'r')
        template_contents = template_file.read()
        template_file.close()
    else:
        # treat `template` as a string
        template_contents = template
    # substitute `kw` into `t` and return it
    return (template_contents % kw)


def mkdir_with_backup(path):
    """
    Like `os.mkdirs`, but if `path` already exists, rename the existing
    one appending a `.NUMBER` suffix.
    """
    if os.path.isdir(path):
        # directory exists; find a suitable extension and rename
        parent_dir = os.path.dirname(path)
        prefix = os.path.dirname(path) + '.'
        p = len(prefix)
        suffix = 1
        for name in [ x for x in os.listdir(parent_dir) if x.startswith(prefix) ]:
            try:
                n = int(name[p:])
                suffix = max(suffix, n)
            except:
                # ignore non-numeric suffixes
                pass
        os.rename(path, "%s.%d" % (path, suffix))
    os.makedirs(path)


def safe_repr(obj):
    """
    Return a string describing Python object `obj`.  Avoids calling
    any Python magic methods, so should be safe to use as a "last
    resort" in implementation of `__str__` and `__repr__` magic.
    """
    return ("<`%s` instance @ %x>" 
            % (obj.__class__.__name__, id(obj)))


def same_docstring_as(referenced_fn):
    """
    Function decorator: sets the docstring of the following function
    to the one of `referenced_fn`.  

    Intended usage is for setting docstrings on methods redefined in
    derived classes, so that they inherit the docstring from the
    corresponding abstract method in the base class.
    """
    def decorate(f):
            f.__doc__ = referenced_fn.__doc__
            return f
    return decorate


def string_to_boolean(word):
    """
    Convert `word` to a Python boolean value and return it.
    The strings `true`, `yes`, `on`, `1` (with any
    capitalization and any amount of leading and trailing
    spaces) are recognized as meaning Python `True`.
    Any other word is considered as boolean `False`.
    """
    if word.strip().lower() in [ 'true', 'yes', 'on', '1' ]:
        return True
    else:
        return False


def to_bytes(s):
    """
    Convert string `s` to an integer number of bytes.  Suffixes like
    'KB', 'MB', 'GB' (up to 'YB'), with or without the trailing 'B',
    are allowed and properly accounted for.  Case is ignored in
    suffixes.

    Examples::

      >>> to_bytes('12')
      12
      >>> to_bytes('12B')
      12
      >>> to_bytes('12KB')
      12000
      >>> to_bytes('1G')
      1000000000

    Binary units 'KiB', 'MiB' etc. are also accepted:

      >>> to_bytes('1KiB')
      1024
      >>> to_bytes('1MiB')
      1048576

    """
    last = -1
    unit = s[last].lower()
    if unit.isdigit():
        # `s` is a integral number
        return int(s)
    if unit == 'b':
        # ignore the the 'b' or 'B' suffix
        last -= 1
        unit = s[last].lower()
    if unit == 'i':
        k = 1024
        last -= 1
        unit = s[last].lower()
    else:
        k = 1000
    # convert the substring of `s` that does not include the suffix
    if unit.isdigit():
        return int(s[0:(last+1)])
    if unit == 'k':
        return int(float(s[0:last])*k)
    if unit == 'm':
        return int(float(s[0:last])*k*k)
    if unit == 'g':
        return int(float(s[0:last])*k*k*k)
    if unit == 't':
        return int(float(s[0:last])*k*k*k*k)
    if unit == 'p':
        return int(float(s[0:last])*k*k*k*k*k)
    if unit == 'e':
        return int(float(s[0:last])*k*k*k*k*k*k)
    if unit == 'z':
        return int(float(s[0:last])*k*k*k*k*k*k*k)
    if unit == 'y':
        return int(float(s[0:last])*k*k*k*k*k*k*k*k)

 
def obtain_file_lock(joblist_location, joblist_lock):
    """
    Lock a file.
    """

    # Obtain lock
    lock_obtained = False
    retries = 3
    default_wait_time = 1


    # if joblist_location does not exist, create it
    if not os.path.exists(joblist_location):
        open(joblist_location, 'w').close()
        gc3libs.log.debug(joblist_location + ' did not exist.  created it.')


    gc3libs.log.debug('trying creating lock for %s in %s',joblist_location,joblist_lock)    

    while lock_obtained == False:
        if ( retries > 0 ):
            try:
                os.link(joblist_location,joblist_lock)
                lock_obtained = True
                break
            except OSError:
                # lock already created; wait
                gc3libs.log.debug('Lock already created; retry later [ %d ]',retries)
                time.sleep(default_wait_time)
                retries = retries - 1
            except:
                gc3libs.log.error('failed obtaining lock due to %s',sys.exc_info()[1])
                raise
        else:
            gc3libs.log.error('could not obtain lock for updating list of jobs')
            break

    return lock_obtained

def release_file_lock(joblist_lock):
    """
    Release locked file.
    """

    try:
        os.remove(joblist_lock)
        return True
    except:
        gc3libs.log.debug('Failed removing lock due to %s',sys.exc_info()[1])
        return False

def date_normalize(date_string):
    """
    Normalizes date format from ARC and SGE sources to  common string
    """
    pass

def notify(job, include_job_results):
    try:
        # create tgz with job information

        job_tarname = gc3libs.Default.NOTIFY_DESTINATIONFOLDER + '/' + job.unique_token + '.tgz'
        tar = tarfile.open(job_tarname, "w:gz")

        if include_job_results:
            try:
                for file in os.listdir(job.job_local_dir):
                    tar.add(os.path.join(job.job_local_dir,file))
            except:
                gc3libs.log.error('Failed while adding files from job_local_dir %s', job.job_local_dir)
        
        # add job object file
        tar.add(os.path.join(gc3libs.Default.JOBS_DIR, job.unique_token))
        
        tar.close()

        # send notification email to gc3admin
        send_mail(gc3libs.Default.NOTIFY_USER_EMAIL,
                   gc3libs.Default.NOTIFY_GC3ADMIN,
                   gc3libs.Default.NOTIFY_SUBJECTS,
                   gc3libs.Default.NOTIFY_MSG,
                   [job_tarname])

        return 0

    except:
        gc3libs.log.error('failed creating report')
        # return 1
        raise

def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
    #assert type(send_to)==list
    #assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg.attach( MIMEText(text) )
    
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)
        
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def send_email(_to,_from,_subject,_msg):
    try:
        _message = email.MIMEText(_msg)
        _message['Subject'] = _subject
        _message['From'] = _from
        _message['To'] = _to

        s = smtplib.SMTP()
        s.connect()
        s.sendmail(_from,[_to],_message.as_string())
        s.quit()

        return 0

    except:
        gc3libs.log.error('Failed sending email [ %s ]',sys.exc_info()[1])
        return 1

if __name__ == '__main__':
    import doctest
    doctest.testmod()
