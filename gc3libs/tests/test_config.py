# test_configparser_subprocess.py
#
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2010-2012, 2015 S3IT, Zentrale Informatik, University of Zurich
#
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

# stdlib imports
from cStringIO import StringIO
import os
import shutil
import tempfile
import re

# nose
from nose.tools import raises, assert_equal

try:
    from nose.tools import assert_is_instance
except ImportError:
    # Python 2.6 does not support assert_is_instance()
    def assert_is_instance(obj, cls):
        assert (isinstance(obj, cls))

# GC3Pie imports
from gc3libs import Run, Application
import gc3libs.config
import gc3libs.core
import gc3libs.template
from gc3libs.quantity import GB, hours
from gc3libs.backends.shellcmd import ShellcmdLrms
from gc3libs.quantity import Memory, Duration


def _setup_config_file(confstr):
    (fd, name) = tempfile.mkstemp()
    f = os.fdopen(fd, 'w+')
    f.write(confstr)
    f.close()
    return name


def test_valid_conf():
    """Test parsing a valid configuration file"""
    tmpfile = _setup_config_file("""
[auth/ssh]
type = ssh
username = gc3pie

[resource/test]
type = shellcmd
auth = ssh
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
override = False
    """)
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        resources = cfg.make_resources(ignore_errors=False)
        # resources are enabled by default
        assert 'test' in resources
        assert_is_instance(resources['test'], ShellcmdLrms)
        # test types
        assert_is_instance(resources['test']['name'], str)
        assert_is_instance(resources['test']['max_cores_per_job'], int)
        assert_is_instance(resources['test']['max_memory_per_core'], Memory)
        assert_is_instance(resources['test']['max_walltime'], Duration)
        assert_is_instance(resources['test']['max_cores'], int)
        assert_is_instance(resources['test']['architecture'], set)
        # test parsed values
        assert_equal(resources['test']['name'], 'test')
        assert_equal(resources['test']['max_cores_per_job'], 2)
        assert_equal(resources['test']['max_memory_per_core'], 2 * GB)
        assert_equal(resources['test']['max_walltime'], 8 * hours)
        assert_equal(resources['test']['max_cores'], 2)
        assert_equal(resources['test']['architecture'],
                     set([Run.Arch.X86_64]))
    finally:
        os.remove(tmpfile)


def test_invalid_confs():
    """Test reading invalid configuration files"""
    invalid_confs = [
        # #0
        (
            'no section header',
            "THIS IS NOT A CONFIG FILE"
        ),
        # #1
        (
            'malformed key=value',
            """
[auth/ssh]
type - ssh
            """
        ),
        # #2
        (
            'missing required key `type` in `auth` section',
            """
[auth/ssh]
#type = ssh
            """
        ),
        # #3
        (
            'missing required key `type` in `resource` section',
            """
[resource/test]
#type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
            """
        ),
        (
            'missing required key `max_cores_per_job` in `resource` section',
            """
[resource/test]
type = shellcmd
frontend = localhost
transport = local
#max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
            """
        ),
        (
            'missing required key `max_memory_per_core` in `resource` section',
            """
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
#max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
        """
        ),
        (
            'missing required key `max_walltime` in `resource` section',
            """
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
#max_walltime = 8 hours
max_cores = 10
architecture = x86_64
            """
        ),
        (
            'missing required key `max_cores` in `resource` section',
            """
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
#max_cores = 10
architecture = x86_64
        """
        ),
        (
            'missing required key `architecture` in `resource` section',
            """
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
#architecture = x86_64
            """
        ),
    ]
    for n, (title, conf) in enumerate(invalid_confs):
        n += 1  # start with `#1` instead of `#0`

        import_invalid_conf.description = (
            test_invalid_confs.__doc__
            + (' #%d: %s (load)' % (n, title))
        )
        yield import_invalid_conf, conf

        read_invalid_conf.description = (
            test_invalid_confs.__doc__
            + (' #%d: %s (merge_file)' % (n, title))
        )
        yield read_invalid_conf, conf

        parse_invalid_conf.description = (
            test_invalid_confs.__doc__
            + (' #%d: %s (_parse)' % (n, title))
        )
        yield parse_invalid_conf, conf


@raises(gc3libs.exceptions.NoConfigurationFile)
def import_invalid_conf(confstr, **extra_args):
    """`load` just ignores invalid input."""
    tmpfile = _setup_config_file(confstr)
    cfg = gc3libs.config.Configuration()
    try:
        cfg.load(tmpfile)
    finally:
        os.remove(tmpfile)
    assert_equal(len(cfg.resources), 0)
    assert_equal(len(cfg.auths), 0)


@raises(gc3libs.exceptions.ConfigurationError)
def read_invalid_conf(confstr, **extra_args):
    """`merge_file` raises a `ConfigurationError` exception on invalid input"""
    tmpfile = _setup_config_file(confstr)
    cfg = gc3libs.config.Configuration()
    try:
        cfg.merge_file(tmpfile)
    finally:
        os.remove(tmpfile)


@raises(gc3libs.exceptions.ConfigurationError)
def parse_invalid_conf(confstr, **extra_args):
    """`_parse` raises a `ConfigurationError` exception on invalid input."""
    cfg = gc3libs.config.Configuration()
    defaults, resources, auths = cfg._parse(StringIO(confstr))


@raises(gc3libs.exceptions.NoAccessibleConfigurationFile)
def test_load_non_existing_file():
    """Test that `Configuration.load()` raises a `NoAccessibleConfigurationFile` exception if no configuration file exists"""
    cfg = gc3libs.config.Configuration('/NON_EXISTING_FILE')


@raises(gc3libs.exceptions.NoAccessibleConfigurationFile)
def test_load_non_readable_file():
    """Test that `Configuration.load()` raises a `NoAccessibleConfigurationFile` exception if no configuration file can be read"""
    with tempfile.NamedTemporaryFile(prefix=__name__) as tmpfile:
        os.chmod(tmpfile.name, 0)
        cfg = gc3libs.config.Configuration(tmpfile.name)


@raises(gc3libs.exceptions.NoValidConfigurationFile)
def test_load_non_valid_file():
    """Test that `Configuration.load()` raises a `NoValidConfigurationFile` exception if no configuration file can be parsed"""
    tmpfile = _setup_config_file("""
This is not a valid configuration file.
""")
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
    finally:
        os.remove(tmpfile)


def test_auth_none():
    """Test that `auth = none` is always available."""
    tmpfile = _setup_config_file("""
[resource/test]
type = pbs
auth = none
frontend = localhost
transport = local
max_cores_per_job = 44
max_memory_per_core = 55 GB
max_walltime = 66 hours
max_cores = 77
architecture = x86_64
    """)
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        auth = cfg.make_auth('none')
        assert_is_instance(auth(), gc3libs.authentication.NoneAuth)
        # if this doesn't raise any error, we're good
        resources = cfg.make_resources(ignore_errors=False)
    finally:
        os.remove(tmpfile)


def test_key_renames():
    """Test that `ncores` is renamed to `max_cores` during parse"""
    tmpfile = _setup_config_file("""
[auth/ssh]
type = ssh
username = gc3pie

[resource/test]
type = shellcmd
auth = ssh
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
# `ncores` renamed to `max_cores`
ncores = 77
architecture = x86_64
override = False
    """)
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        resources = cfg.make_resources(ignore_errors=False)
        assert 'ncores' not in resources['test']
        assert 'max_cores' in resources['test']
        assert_equal(resources['test']['max_cores'], 77)
    finally:
        os.remove(tmpfile)


class TestReadMultiple(object):

    def setUp(self):
        self.f1 = _setup_config_file("""
[resource/localhost]
seq = 1
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
        """)
        self.f2 = _setup_config_file("""
[resource/localhost]
seq = 1
foo = 2
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
        """)
        self.f3 = _setup_config_file("""
[resource/localhost]
seq = 3
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
        """)

    def tearDown(self):
        os.remove(self.f1)
        os.remove(self.f2)
        os.remove(self.f3)

    def test_read_config_multiple(self):
        """Test that `Configuration` aggregates multiple files"""
        cfg = gc3libs.config.Configuration(self.f1, self.f2, self.f3)
        assert_equal(cfg.resources['localhost']['seq'], '3')
        assert_equal(cfg.resources['localhost']['foo'], '2')

    def test_load_multiple(self):
        """Test that `Configuration.load` aggregates multiple files"""
        cfg = gc3libs.config.Configuration()
        cfg.load(self.f1, self.f2, self.f3)
        assert_equal(cfg.resources['localhost']['seq'], '3')
        assert_equal(cfg.resources['localhost']['foo'], '2')

    def test_merge_file_multiple(self):
        """Test that `Configuration.load` aggregates multiple files"""
        cfg = gc3libs.config.Configuration()
        cfg.merge_file(self.f1)
        cfg.merge_file(self.f2)
        cfg.merge_file(self.f3)
        assert_equal(cfg.resources['localhost']['seq'], '3')
        assert_equal(cfg.resources['localhost']['foo'], '2')


@raises(gc3libs.exceptions.NoConfigurationFile)
def test_no_valid_config1():
    """`Configuration.load` raises an exception if called with no arguments"""
    cfg = gc3libs.config.Configuration()
    cfg.load()


@raises(gc3libs.exceptions.NoConfigurationFile)
def test_no_valid_config2():
    """Test that `Configuration.load` raises an exception if none of the
    arguments is a valid config file"""
    f1 = _setup_config_file("INVALID INPUT")
    f2 = _setup_config_file("INVALID INPUT")
    cfg = gc3libs.config.Configuration()
    try:
        cfg.load(f1, f2)
    finally:
        os.remove(f1)
        os.remove(f2)


def test_valid_architectures():
    """Test that valid architecture strings are parsed correctly"""
    test_cases = [
        # a sample of the architecture strings that we accept
        ('x86_64', [gc3libs.Run.Arch.X86_64]),
        ('x86 64-bit', [gc3libs.Run.Arch.X86_64]),
        ('64bits x86', [gc3libs.Run.Arch.X86_64]),
        ('amd64', [gc3libs.Run.Arch.X86_64]),
        ('AMD64', [gc3libs.Run.Arch.X86_64]),
        ('intel 64', [gc3libs.Run.Arch.X86_64]),
        ('emt64', [gc3libs.Run.Arch.X86_64]),
        ('64 bits', [gc3libs.Run.Arch.X86_64]),
        ('64-BIT', [gc3libs.Run.Arch.X86_64]),

        ('x86 32-bit', [gc3libs.Run.Arch.X86_32]),
        ('32bits x86', [gc3libs.Run.Arch.X86_32]),
        ('i386', [gc3libs.Run.Arch.X86_32]),
        ('i486', [gc3libs.Run.Arch.X86_32]),
        ('i586', [gc3libs.Run.Arch.X86_32]),
        ('i686', [gc3libs.Run.Arch.X86_32]),
        ('32 bits', [gc3libs.Run.Arch.X86_32]),
        ('32-bit', [gc3libs.Run.Arch.X86_32]),

        ('32bit, 64bit', [gc3libs.Run.Arch.X86_64, gc3libs.Run.Arch.X86_32]),
        ('64bit, x86_64', [gc3libs.Run.Arch.X86_64]),
    ]
    config_template = gc3libs.template.Template("""
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = ${arch}
""")
    for arch, result in test_cases:
        # display complete title for each generated test case
        _check_parse_arch.description = (
            test_valid_architectures.__doc__ +
            (': %s -> %s' % (str(arch), str(result))))
        yield (_check_parse_arch,
               config_template.substitute(arch=arch),
               result)


def _check_parse_arch(confstr, result):
    cfg = gc3libs.config.Configuration()
    defaults, resources, auths = cfg._parse(StringIO(confstr))
    assert_is_instance(resources['test']['architecture'], set)
    assert_equal(resources['test']['architecture'], set(result))


def test_invalid_architectures():
    """Invalid arch. strings should be rejected with `ConfigurationError`

    """
    test_cases = [
        '96bits', '31-BITS', 'amd65', 'xyzzy'
    ]
    config_template = gc3libs.template.Template("""
[resource/test]
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 10
architecture = ${arch}
""")
    for arch in test_cases:
        # display complete title for each generated test case
        _check_parse_arch.description = (
            test_invalid_architectures.__doc__ + (': %s' % arch))
        yield (raises(
            gc3libs.exceptions.ConfigurationError)(_check_parse_arch),
            config_template.substitute(arch=arch), 'should not be used')


class TestPrologueEpilogueScripts(object):

    """
    Test `prologue` and `epilogue` options for batch backends
    """

    def setUp(self):
        # setup conf dir and conf file
        self.files_to_remove = []

        self.conftmpdir = tempfile.mkdtemp()
        self.tmpdir = tempfile.mkdtemp()
        # setup config file and sctipts
        cfgfname = os.path.join(self.conftmpdir, 'gc3pie.conf')
        self.files_to_remove.append(self.conftmpdir)
        self.files_to_remove.append(self.tmpdir)

        cfgstring = """
[auth/ssh]
type = ssh
username = gc3pie

[resource/test]
type = shellcmd
auth = ssh
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
prologue = %(tmpdir)s/scripts/shellcmd_pre.sh
epilogue = %(tmpdir)s/scripts/shellcmd_post.sh
myapp_prologue = %(tmpdir)s/scripts/myapp_shellcmd_pre.sh
myapp_epilogue = %(tmpdir)s/scripts/myapp_shellcmd_post.sh
override = False

[resource/testpbs]
type = pbs
auth = ssh
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
prologue = %(tmpdir)s/scripts/shellcmd_pre.sh
epilogue = %(tmpdir)s/scripts/shellcmd_post.sh
myapp_prologue = %(tmpdir)s/scripts/myapp_shellcmd_pre.sh
myapp_epilogue = %(tmpdir)s/scripts/myapp_shellcmd_post.sh
app2_prologue_content = echo prologue app2
app2_epilogue_content = echo epilogue app2
""" % {'tmpdir': self.tmpdir}

        fdcfg = open(cfgfname, 'w')
        fdcfg.write(cfgstring)
        fdcfg.close()
        self.cfg = gc3libs.config.Configuration(
            cfgfname,
            auto_enable_auth=True)

        self.scripts = [
            'prologue',
            'epilogue',
            'myapp_prologue',
            'myapp_epilogue']
        os.mkdir(os.path.join(self.tmpdir, 'scripts'))
        for k, v in self.cfg['resources']['test'].iteritems():
            if k in self.scripts:
                scriptfd = open(os.path.join(self.tmpdir, v), 'w')
                scriptfd.write('echo %s' % k)
                scriptfd.close()

        self.cfg = gc3libs.config.Configuration(cfgfname)
        # self.resources = self.cfg.make_resources(ignore_errors=False)
        # assert_equal(resources['test']['prologue'],
        #              'scripts/shellcmd_pre.sh')

    def tearDown(self):
        for dirname in self.files_to_remove:
            shutil.rmtree(dirname)

    def test_scriptfiles_are_abs(self):
        """Test that prologue and epilogue scripts are absolute pathnames"""
        self.core = gc3libs.core.Core(self.cfg)
        for resource in self.core.get_resources():
            for (k, v) in resource.iteritems():
                if k not in ['prologue', 'epilogue',
                             'myapp_prologue', 'myapp_epilogue']:
                    continue
                assert os.path.isfile(v)
                assert os.path.isabs(v)
                assert_equal(os.path.abspath(v),
                             v)

    def test_pbs_prologue_and_epilogue_contents_when_files(self):
        """Prologue and epilogue scripts are inserted in the submission script
        """
        # Ugly hack. We have to list the job dirs to check which one
        # is the new one.
        jobdir = os.path.expanduser('~/.gc3pie_jobs')
        jobs = []
        if os.path.isdir(jobdir):
            jobs = os.listdir(jobdir)
        app = Application(['/bin/true'], [], [], '')
        self.core = gc3libs.core.Core(self.cfg)
        self.core.select_resource('testpbs')
        try:
            self.core.submit(app)
        except Exception:
            # it is normal to have an error since we don't probably
            # run a pbs server in this machine.
            pass

        newjobs = [d for d in os.listdir(jobdir) if d not in jobs]

        # There must be only one more job...
        assert_equal(len(newjobs), 1)

        newjobdir = os.path.join(jobdir, newjobs[0])
        self.files_to_remove.append(newjobdir)

        # and only one file in it
        assert_equal(len(os.listdir(newjobdir)), 1)

        # Check the content of the script file
        scriptfname = os.path.join(newjobdir, (os.listdir(newjobdir)[0]))
        scriptfile = open(scriptfname)
        assert re.match(
            "#!/bin/sh.*# prologue file `.*` BEGIN.*echo prologue.*# prologue"
            " file END.*/bin/true.*# epilogue file `.*` BEGIN.*echo epilogue"
            ".*# epilogue file END",
            scriptfile.read(),
            re.DOTALL | re.M)
        scriptfile.close()

        # kill the job
        if app.execution.state != Run.State.NEW:
            self.core.kill(app)

    def test_pbs_prologue_and_epilogue_contents_when_not_files(self):
        """Prologue and epilogue scripts are inserted in the submission script
        """
        # Ugly hack. We have to list the job dirs to check which one
        # is the new one.
        jobdir = os.path.expanduser('~/.gc3pie_jobs')
        jobs = []
        if os.path.isdir(jobdir):
            jobs = os.listdir(jobdir)
        app = Application(['/bin/true'], [], [], '')
        app.application_name = 'app2'
        self.core = gc3libs.core.Core(self.cfg)
        self.core.select_resource('testpbs')
        try:
            self.core.submit(app)
        except Exception:
            # it is normal to have an error since we don't probably
            # run a pbs server in this machine.
            pass

        newjobs = [d for d in os.listdir(jobdir) if d not in jobs]

        # There must be only one more job...
        assert_equal(len(newjobs), 1)

        newjobdir = os.path.join(jobdir, newjobs[0])
        self.files_to_remove.append(newjobdir)

        # and only one file in it
        assert_equal(len(os.listdir(newjobdir)), 1)

        # Check the content of the script file
        scriptfname = os.path.join(newjobdir, (os.listdir(newjobdir)[0]))
        scriptfile = open(scriptfname)
        assert re.match(
            "#!/bin/sh.*"
            "# prologue file `.*` BEGIN.*"
            "echo prologue.*"
            "# prologue file END.*"
            "# inline script BEGIN.*"
            "echo prologue app2.*"
            "# inline script END.*"
            "/bin/true.*"
            "# epilogue file `.*` BEGIN.*"
            "echo epilogue.*"
            "# epilogue file END.*"
            "# inline script BEGIN.*"
            "echo epilogue app2.*"
            "# inline script END",
            scriptfile.read(),
            re.DOTALL | re.M)
        scriptfile.close()

        # kill the job
        if app.execution.state != Run.State.NEW:
            self.core.kill(app)

    def test_prologue_epilogue_issue_352(self):
        cfgstring = """
[auth/ssh]
type = ssh
username = gc3pie

[resource/%s]
type = shellcmd
auth = ssh
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
override = False
"""
        dir1, dir2 = tempfile.mkdtemp(), tempfile.mkdtemp()
        self.files_to_remove.extend([dir1, dir2])
        cfgfilenames = []

        for d in [dir1, dir2]:
            cfgfname = os.path.join(d, 'gc3pie.conf')
            fdcfg = open(cfgfname, 'w')
            fdcfg.write(cfgstring % os.path.basename(d))
            fdcfg.close()
            cfgfilenames.append(cfgfname)

        self.cfg = gc3libs.config.Configuration(
            cfgfilenames[0],
            self.cfg.cfgfiles[0],
            cfgfilenames[1], auto_enable_auth=True)

        self.core = gc3libs.core.Core(self.cfg)
        for resource in self.core.get_resources():
            for (k, v) in resource.iteritems():
                if k not in ['prologue', 'epilogue',
                             'myapp_prologue', 'myapp_epilogue']:
                    continue
                assert os.path.isabs(v)
                assert os.path.isfile(v)
                assert_equal(os.path.abspath(v),
                             v)


@raises(gc3libs.exceptions.ConfigurationError)
def test_invalid_resource_type():
    """Test parsing a configuration file with an unknown resource type."""
    tmpfile = _setup_config_file("""
[resource/test]
type = noop
auth = none
transport = local
max_cores_per_job = 1
max_memory_per_core = 1
max_walltime = 8
max_cores = 2
architecture = x86_64
    """)
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        resources = cfg.make_resources(ignore_errors=False)
    finally:
        os.remove(tmpfile)


def test_removed_arc0_resource_type():
    """Test parsing a configuration file with the removed `arc0` resource type."""
    _test_removed_resource_type('arc0')

def test_removed_arc1_resource_type():
    """Test parsing a configuration file with the removed `arc1` resource type."""
    _test_removed_resource_type('arc1')

def _test_removed_resource_type(kind):
    name = ('test_' + kind)
    tmpfile = _setup_config_file("""
[resource/{name}]
type = {kind}
auth = none
transport = local
max_cores_per_job = 11
max_memory_per_core = 22 GB
max_walltime = 33 hours
max_cores = 44
architecture = x86_64
    """.format(name=name, kind=kind))
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        resources = cfg.make_resources(ignore_errors=False)
        assert name not in resources
    finally:
        os.remove(tmpfile)


def test_additional_backend():
    """Test instanciating a non-std backend."""
    tmpfile = _setup_config_file("""
[resource/test]
type = noop
auth = none
transport = local
max_cores_per_job = 1
max_memory_per_core = 1
max_walltime = 8
max_cores = 2
architecture = x86_64
    """)
    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        cfg.TYPE_CONSTRUCTOR_MAP['noop'] = ('gc3libs.backends.noop', 'NoOpLrms')
        resources = cfg.make_resources(ignore_errors=False)
        # resources are enabled by default
        assert 'test' in resources
        from gc3libs.backends.noop import NoOpLrms
        assert_is_instance(resources['test'], NoOpLrms)
        # test types
    finally:
        # since TYPE_CONSTRUCTOR_MAP is a class-level variable, we
        # need to clean up otherwise other tests will see the No-Op
        # backend
        del cfg.TYPE_CONSTRUCTOR_MAP['noop']
        os.remove(tmpfile)


def test_resource_definition_via_dict():
    """Test programmatic resource definition (as opposed to reading a config file)."""
    cfg = gc3libs.config.Configuration()
    # define resource
    name = 'test'
    cfg.resources[name].update(
        name=name,
        type='shellcmd',
        auth='none',
        transport='local',
        max_cores_per_job=1,
        max_memory_per_core=1*GB,
        max_walltime=8*hours,
        max_cores=2,
        architecture=Run.Arch.X86_64,
    )
    # make it
    resources = cfg.make_resources(ignore_errors=False)
    # check result
    resource = resources[name]
    assert_equal(resource.name, name)
    assert_is_instance(resource,
                       gc3libs.backends.shellcmd.ShellcmdLrms)
    assert_is_instance(resource.transport,
                       gc3libs.backends.transport.LocalTransport)
    assert_equal(resource.max_cores_per_job, 1)
    assert_equal(resource.max_memory_per_core, 1*GB)
    assert_equal(resource.max_walltime, 8*hours)
    assert_equal(resource.max_cores, 2)
    assert_equal(resource.architecture, Run.Arch.X86_64)


def test_multiple_instanciation(num_resources=3):
    """Check that multiple resources of the same type can be instanciated."""
    cfg = gc3libs.config.Configuration()
    for n in range(num_resources):
        name = ('test%d' % (n+1))
        cfg.resources[name].update(
            name=name,
            type='shellcmd',
            auth='none',
            transport='local',
            max_cores_per_job=1,
            max_memory_per_core=1*GB,
            max_walltime=8*hours,
            max_cores=2,
            architecture=Run.Arch.X86_64,
        )
    resources = cfg.make_resources(ignore_errors=False)
    for n in range(num_resources):
        name = ('test%d' % (n+1))
        resource = resources[name]
        assert_equal(resource.name, name)
        assert_is_instance(resource, gc3libs.backends.shellcmd.ShellcmdLrms)


if __name__ == "__main__":
    import nose
    nose.runmodule()
