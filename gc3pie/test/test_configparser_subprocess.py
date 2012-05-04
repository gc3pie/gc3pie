# -*- coding: utf-8 -*-# 
# @(#)test_configparser_subprocess.py
# 
# 
#  Copyright (C) 2010, 2011, 2012 GC3, University of Zurich
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

import gc3libs.core

import tempfile, os
import pytest

def _setup_config_file(confstring):
    (fd, name) = tempfile.mkstemp()
    f = os.fdopen(fd, 'r+')
    f.write(confstring)
    f.close()
    return name

def test_conf_shellcmd_valid():
    """test a valid configuration file"""
    confstring = """
[auth/anto_ssh]
type = ssh
username = gc3pie

[resource/localhost]
enabled = true
type = shellcmd
auth = anto_ssh
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
ncores = 2
"""
    # This should be valid
    fname = _setup_config_file(confstring+"architecture = x86_64\n")
    (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    os.remove(fname)
    assert res[0].enabled
    assert res[0].type == 'shellcmd'
    assert res[0].architecture == 'x86_64'
    assert res[0].transport == 'local'

    # This architecture value is invalid
    fname = _setup_config_file(confstring+"architecture = 32bit\n")
    try:
        (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    except gc3libs.ConfigurationError:
        os.remove(fname)
        assert True

    fname = _setup_config_file(confstring+"architecture = i386\n")
    (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    os.remove(fname)
    assert res[0].enabled
    assert res[0].type == 'shellcmd'
    assert res[0].architecture == 'i386'
    assert res[0].transport == 'local'

def test_conf_subprocess_valid():
    """test a valid configuration file"""
    confstring = """
[auth/anto_ssh]
type = ssh
username = gc3pie

[resource/localhost]
enabled = true
auth = anto_ssh
type = subprocess
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
ncores = 2
"""
    # This should be valid
    fname = _setup_config_file(confstring+"architecture = x86_64\n")
    (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    os.remove(fname)
    assert res[0].enabled
    assert res[0].type == 'subprocess'
    assert res[0].architecture == 'x86_64'
    assert res[0].transport == 'local'

    # This architecture value is invalid
    fname = _setup_config_file(confstring+"architecture = 32bit\n")
    try:
        (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    except gc3libs.ConfigurationError:
        os.remove(fname)
        assert True

    fname = _setup_config_file(confstring+"architecture = i386\n")
    (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
    os.remove(fname)
    assert res[0].enabled
    assert res[0].type == 'subprocess'
    assert res[0].architecture == 'i386'
    assert res[0].transport == 'local'

def test_conf_subprocess_invalid():
    """Test for needed configuration options"""
    confstring = """
[auth/anto_ssh]
type = ssh
username = gc3pie

[resource/localhost]
enabled = true
frontend = localhost,
"""
    
    opts = [
        "type = subprocess",
        "max_cores_per_job = 2",
        "max_memory_per_core = 2",
        "max_walltime = 8",
        "ncores = 2",
        "architecture = x86_64",
        ]

    for i in range(len(opts)):
        l = opts[:i]+opts[i+1:]
        m = confstring + "\n".join(l)
        fname = _setup_config_file(m)
        (res, auth, auto_enable) =  gc3libs.core.import_config([fname])
        os.remove(fname)
        assert res == []


if __name__ == "__main__":
    from common import run_my_tests
    run_my_tests(locals())
