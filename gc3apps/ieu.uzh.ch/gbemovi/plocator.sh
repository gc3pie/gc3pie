#!/bin/bash
#
#
#  Copyright (C) 2016, S3IT, University of Zurich. All rights
#  reserved.
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
#

## main

# Some ImageJ functions need to have an X11 server running.
export DISPLAY=:1
xwininfo -root >& /dev/null || Xvfb $DISPLAY -auth /dev/null >& /dev/null &

echo "executing Rscript bemovi.R $@"
exec Rscript bemovi.R $@
