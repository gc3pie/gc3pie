#! /usr/bin/env python
#
"""
"""
from __future__ import division
# Copyright (C) 2012-2013,  University of Zurich. All rights reserved.
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
from future import standard_library
standard_library.install_aliases()
from past.utils import old_div
__docformat__ = 'reStructuredText'

from __future__ import absolute_import, print_function, unicode_literals
import argparse
import json
import logging
import mimetypes
import os
import sys
import time
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse

try:
    import magic
except ImportError:
    magic = None

log = logging.getLogger()
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.DEBUG)

def open_http(url):
    log.info("Sending request to %s", url.geturl())
    return urllib.request.urlopen(url.geturl())


def open_swift(url, method='get', content_type=None, content_length=0, content_data=None):
    username, tenant = url.username.split('+')
    password = url.password
    if url.scheme in ['swifts', 'swts']:
        auth_url = 'https://%s' % url.hostname
    else:
        auth_url = 'http://%s' % url.hostname
    query = url.query.split('&')
    container = query[0]
    object_name = query[1].rsplit('=',1)[-1]
    if url.port:
        auth_url += ':%d' % url.port
    if url.path:
        auth_url += url.path

    # Get a token from keystone
    data = json.dumps(
        {
            'auth' : {
                'tenantName' : tenant,
                'passwordCredentials' : {
                    'username' : username,
                    'password' : password
                }
            }
        }
    )
    token_url = auth_url+'/tokens'
    log.info("Getting token from '%s' for user '%s', tenant '%s'",
             token_url, username, tenant)

    kreq = urllib.request.Request(token_url, data)
    kreq.add_header("Content-type", "application/json")

    fp = urllib.request.urlopen(kreq)
    authresp = json.loads(fp.read())
    fp.close()

    token = authresp['access']['token']['id']

    # Get the storage URL
    for endpoint in authresp['access']['serviceCatalog']:
        if endpoint['type'] == 'object-store':
            storage_url = endpoint['endpoints'][0]['publicURL']
    log.info("Token recovered: %s, storage_url: %s",
              token, storage_url)
    # Get the object from swift
    object_url = os.path.join(
        storage_url,
        container,
        object_name).encode('utf-8')

    # If this is a GET, we just download it
    sreq = urllib.request.Request(object_url, content_data)
    sreq.add_header('X-Auth-Token', token)

    # If it's a PUT, we also need to set more information
    if method.upper() == 'PUT':
        sreq.get_method = lambda: 'PUT'
        sreq.add_header('Content-Length', len(content_data))
        if content_type:
            sreq.add_header('Content-Type', content_type)
        else:
            sreq.add_header('Content-Type', 'application/octect-stream')
    log.info("Headers: %s" % sreq.headers)
    log.info("Sending request to %s", object_url)
    return urllib.request.urlopen(sreq)


def download_file(url, outfile, bufsize=2**20):
    url = urllib2.urlparse.urlparse(url)
    stime = time.time()
    if url.scheme in ['http', 'https']:
        fd = open_http(url)
    elif url.scheme in ['swift', 'swt', 'swifts', 'swts']:
        fd = open_swift(url)
    if not fd:
        log.error("Unrecognized scheme %s", url.scheme)
        return 1

    with open(outfile, 'w') as outfd:
        data = fd.read(bufsize)
        while data:
            outfd.write(data)
            data = fd.read(bufsize)
    fd.close()
    etime = time.time()
    log.info("File '%s' downloaded to '%s' in %f seconds (%d bytes/s)", url.geturl(), outfile, etime-stime, old_div(os.stat(outfile).st_size,(etime-stime)))

def upload_file(url, local, bufsize=2**20):
    url = urllib2.urlparse.urlparse(url)
    stime = time.time()
    if url.scheme in ['http', 'https']:
        log.error("Unable to upload to '%s'", url.scheme)
        return 1

    if url.scheme in ['swift', 'swifts', 'swt', 'swts']:
        # Guess the content-type of the file.
        try:
            # magic module might not be installed, but it works way
            # better than `mimetypes`
            m = magic.open(magic.MAGIC_MIME)
            m.load()
            ctype = m.file(local)
        except:
            # revert back to mime types, which is only checking the
            # file extension
            ctype = mimetypes.guess_type(local)[0]
        # length can be
        clength = os.stat(local).st_size
        with open(local, 'r') as localfd:
            fd = open_swift(url,
                            method='put',
                            content_type=ctype,
                            content_length=clength,
                            content_data=localfd.read())
    etime = time.time()
    log.info("File '%s' uploaded to '%s' in %f seconds (%d bytes/s)", local, url.geturl(), etime-stime, old_div(clength,(etime-stime)))


## main: run tests

if "__main__" == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['upload', 'download'])
    parser.add_argument('remote')
    parser.add_argument('local')
    cfg = parser.parse_args()
    if cfg.action == 'download':
        sys.exit(download_file(cfg.remote, cfg.local))
    else:
        if not os.path.isfile(cfg.local):
            parser.error("File '%s' not found. Unable to upload it." % cfg.local)
        sys.exit(upload_file(cfg.remote, cfg.local))
