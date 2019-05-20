# test_workflow.py
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2015, 2016  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library
standard_library.install_aliases()
from io import StringIO
import os
import shutil
import tempfile
import re

# GC3Pie imports
from gc3libs import Run
from gc3libs.workflow import SequentialTaskCollection, StagedTaskCollection, StopOnError

from gc3libs.testing.helpers import SuccessfulApp, UnsuccessfulApp, temporary_core


def test_staged_task_collection_progress():
    class ThreeStageCollection(StagedTaskCollection):
        def stage0(self):
            return SuccessfulApp()
        def stage1(self):
            return SuccessfulApp()
        def stage2(self):
            return UnsuccessfulApp()

    with temporary_core() as core:
        coll = ThreeStageCollection()
        coll.attach(core)
        coll.submit()
        assert coll.execution.state == Run.State.SUBMITTED

        # first task is successful
        while coll.tasks[0].execution.state != Run.State.TERMINATED:
            coll.progress()
        assert coll.execution.state in [Run.State.SUBMITTED, Run.State.RUNNING]
        #assert_equal(coll.execution.exitcode, 0)

        # second task is successful
        while coll.tasks[1].execution.state != Run.State.TERMINATED:
            coll.progress()
        #assert_equal(coll.execution.state, Run.State.RUNNING)
        #assert_equal(coll.execution.exitcode, 0)

        # third task is unsuccessful
        while coll.tasks[2].execution.state != Run.State.TERMINATED:
            coll.progress()
        assert coll.execution.state == Run.State.TERMINATED
        assert coll.execution.exitcode == 1


def test_staged_task_collection_stage():
    class TwoStageCollection(StagedTaskCollection):
        def stage0(self):
            return SuccessfulApp(name='stage0')
        def stage1(self):
            return UnsuccessfulApp(name='stage1')

    with temporary_core() as core:
        coll = TwoStageCollection()
        coll.attach(core)
        coll.submit()
        stage = coll.stage()
        assert isinstance(stage, SuccessfulApp), ("stage=%r" % (stage,))
        assert stage.jobname == 'stage0'

        # advance to next task
        while coll.tasks[0].execution.state != Run.State.TERMINATED:
            coll.progress()
        stage = coll.stage()
        assert isinstance(stage, UnsuccessfulApp)
        assert stage.jobname == 'stage1'
