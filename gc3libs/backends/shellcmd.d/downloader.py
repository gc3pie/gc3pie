#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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

import argparse
import urllib
import urllib2
import json
import os


def open_http(url):
    return urllib2.urlopen(url.geturl())


def open_swift(url):
    username, tenant = url.username.split('+')
    password = url.password
    auth_url = 'https://%s' % url.hostname
    query = url.query.split('&')
    container = query[0]
    object_name = query[1].rsplit('=',1)[-1]
    if url.port:
        auth_url += ':%d' % url.port
    if url.path:
        auth_url += url.path

    # Get a token from keystone
    data = json.dumps({'auth' : {'tenantName' : tenant, 'passwordCredentials' : {'username' : username, 'password' : password}}})
    kreq = urllib2.Request(auth_url+'/tokens', data)
    kreq.add_header("Content-type", "application/json")
    
    fp = urllib2.urlopen(kreq)
    authresp = json.loads(fp.read())
    fp.close()

    token = authresp['access']['token']['id']

    # Get the storage URL
    for endpoint in authresp['access']['serviceCatalog']:
        if endpoint['type'] == 'object-store':
            storage_url = endpoint['endpoints'][0]['publicURL']
    
    # Get the object from swift
    object_url = os.path.join(
        storage_url,
        container,
        object_name)
    sreq = urllib2.Request(object_url)
    sreq.add_header('X-Auth-Token', token)
    return urllib2.urlopen(sreq)


def download_file(url, outfile, bufsize=2**20):
    url = urllib2.urlparse.urlparse(url)
    if url.scheme in ['http', 'https']:
        fd = open_http(url)
    elif url.scheme == 'swift':
        fd = open_swift(url)

    with open(outfile, 'w') as outfd:
        data = fd.read(bufsize)
        while data:
            outfd.write(data)
            data = fd.read(bufsize)
    fd.close()

## main: run tests

if "__main__" == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('output')
    cfg = parser.parse_args()
    download_file(cfg.input, cfg.output)
