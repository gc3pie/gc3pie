# Copyright (C) 2003  CAMP
# Please see the accompanying LICENSE file for further information.

version = '3.3.1'

from os import path
try:
    from subprocess import Popen, PIPE
except ImportError:
    from os import popen3
else:
    def popen3(cmd):
        p = Popen(cmd, shell=True, close_fds=True,
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        return p.stdin, p.stdout, p.stderr

def write_svnrevision(output):
    f = open(path.join('ase', 'svnrevision.py'),'w')
    f.write('svnrevision = "%s"\n' % output)
    f.close()
    print 'svnrevision = ' +output+' written to ase/svnrevision.py'
    # assert svn:ignore property if the installation is under svn control
    # because svnrevision.py has to be ignored by svn!
    cmd = popen3('svn propset svn:ignore svnrevision.py ase')[1]
    output = cmd.read()
    cmd.close()

def read_svnrevision(filename):
    f = open(filename,'r')
    s = f.read()
    f.close()
    exec(s)
##    print 'svnrevision = ' +svnrevision+' read from '+filename # MDTMP
    return svnrevision

def get_svnversion(dir='ase'):
    # try to get the last svn revision number from svnversion
    cmd = popen3('svnversion -n '+dir)[1] # assert that we are in ase project
    output = cmd.read()
    cmd.close()
    svnrevisionfile = path.join(dir, 'svnrevision.py')
    # we build from exported source (e.g. rpmbuild)
    if output.startswith('exported') and path.isfile(svnrevisionfile):
        # read the last svn revision number
        output = read_svnrevision(svnrevisionfile)
    return output

def svnversion(version):
    revision = version # the default output of this function
    try:
        # try to get the last svn revision number from ase/svnrevision.py
        from ase.svnrevision import svnrevision
        # ase is installed, here:
        from ase import __file__ as f
        # get the last svn revision number from svnversion ase/ase dir
        asedir = path.abspath(path.dirname(f))
        # assert we have ase/svnrevision.py file
        svnrevisionfile = path.join(asedir, 'svnrevision.py')
        # we build from exported source (e.g. rpmbuild)
        assert path.isfile(svnrevisionfile)
        if path.split(
            path.abspath(path.join(asedir, path.pardir)))[1] == 'ase':
            # or from svnversion ase dir
            asedir = path.join(asedir, path.pardir)
        # version.py can be called from any place so we need to specify asedir
        output = get_svnversion(asedir)
        if (output != '') and (output != svnrevision) and (not output.startswith('exported')):
            # output the current svn revision number into ase/svnrevision.py
            svnrevision = output
        version = version+'.'+svnrevision
    except:
        # ase is not installed:
        # try to get the last svn revision number from svnversion
        output = get_svnversion()
        if (output != '') and (not output.startswith('exported')):
            # svnversion exists:
            # we are sure to have the write access as what we are doing
            # is running setup.py now (even during rpmbuild)!
            # save the current svn revision number into ase/svnrevision.py
            write_svnrevision(output)
            svnrevision = output
            version = version+'.'+svnrevision
    ##
    return version

version = svnversion(version)
