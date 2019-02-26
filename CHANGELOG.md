# [Changelog](https://github.com/uzh/gc3pie/releases)

## [2.4.3](https://github.com/uzh/gc3pie/compare/2.4.2...2.4.3)

* [4ba27c0](https://github.com/uzh/gc3pie/commit/4ba27c0) fixes #530 encountered bug while running installer: OSError while trying to execute virtualenv.py PYTHONDONTWRITEBYTECODE environment variable is not compatible with setuptools added --distribute to install to solve, might need further investigation
* [73dd500](https://github.com/uzh/gc3pie/commit/73dd500) fixes #555 fixes #562 fixes #575
* [81da1d4](https://github.com/uzh/gc3pie/commit/81da1d4) #562 fix
* [ccab0ef](https://github.com/uzh/gc3pie/commit/ccab0ef) #575 fix
* [76b1825](https://github.com/uzh/gc3pie/commit/76b1825) Added install to local Anaconda
* [4eb3222](https://github.com/uzh/gc3pie/commit/4eb3222) Fixes 'illegal -N value'
* [4e912f8](https://github.com/uzh/gc3pie/commit/4e912f8) Update "GC3Pie for End-Users" slides.
* [4f2ad31](https://github.com/uzh/gc3pie/commit/4f2ad31) Update link to Gitter room
* [597670e](https://github.com/uzh/gc3pie/commit/597670e) Move Gitter badge close to title.
* [75cd01a](https://github.com/uzh/gc3pie/commit/75cd01a) Add Gitter badge (#588)
* [eaa645f](https://github.com/uzh/gc3pie/commit/eaa645f) gbeast: Use the `-resume` option if a `.trees` file is already in input directory.
* [9bd8eee](https://github.com/uzh/gc3pie/commit/9bd8eee) Fix explanation of early termination of `StagedTaskCollection`.
* [c6d3a06](https://github.com/uzh/gc3pie/commit/c6d3a06) Make scripts in `examples/tutorial` executable.
* [103201a](https://github.com/uzh/gc3pie/commit/103201a) Fix old-style task initialization in `examples/tutorial/exercise_E.py`.
* [75661ae](https://github.com/uzh/gc3pie/commit/75661ae) Use `try`/`except` instead of testing for attribute existence.
* [2326644](https://github.com/uzh/gc3pie/commit/2326644) Only manipulate `.output_dir` if the task is expected to create output files.
* [beb44c3](https://github.com/uzh/gc3pie/commit/beb44c3) Fix initialization of `StagedTaskCollection` subclasses in examples.
* [f3c7449](https://github.com/uzh/gc3pie/commit/f3c7449) Cosmetic changes in docstring and comments to `StagedTaskCollection`.
* [4aa94cf](https://github.com/uzh/gc3pie/commit/4aa94cf) Fix "NameError: global name 'InternalError' is not defined" at core.py line 1390.
* [1f5d4d7](https://github.com/uzh/gc3pie/commit/1f5d4d7) Split the "More workflows" presentation into new part 10 and part 11.
* [5a41e9b](https://github.com/uzh/gc3pie/commit/5a41e9b) Fix URL of slide decks.
* [cfc610a](https://github.com/uzh/gc3pie/commit/cfc610a) New exercise with asset price simulation in part 6 of the programmers' "workflow" tutorial.
* [7c86d1a](https://github.com/uzh/gc3pie/commit/7c86d1a) fixed link reference for Introduction to GC3Pie
* [2835637](https://github.com/uzh/gc3pie/commit/2835637) Correct solution to Ex. 4.B to with with `blastp` instead of `blastpgp`.
* [a815644](https://github.com/uzh/gc3pie/commit/a815644) Expand part 03 with a longer list of commands.
* [84ce532](https://github.com/uzh/gc3pie/commit/84ce532) Add slide on option `-r`/`--resource` to part 02.
* [fb74408](https://github.com/uzh/gc3pie/commit/fb74408) "workflow" tutorial: misc small changes.
* [15d73d4](https://github.com/uzh/gc3pie/commit/15d73d4) Revise workflow tutorial part 04, and add shorter FAA files for the BLAST example.
* [1f15b4e](https://github.com/uzh/gc3pie/commit/1f15b4e) Restore meaning of `Application.requested_walltime==None` as "unlimited/unspecified duration".
* [3e1a1e9](https://github.com/uzh/gc3pie/commit/3e1a1e9) tox.ini: Install setuptools >= 21.0.0
* [0105aca](https://github.com/uzh/gc3pie/commit/0105aca) Change date on the slides to Nov. 14--17, 2016
* [d27bdb5](https://github.com/uzh/gc3pie/commit/d27bdb5) Update tinyurl link for the "workflows" tutorial.
* [847f4e8](https://github.com/uzh/gc3pie/commit/847f4e8) Revise part04 of the programmers' "workflow" tutorial.
* [c42a196](https://github.com/uzh/gc3pie/commit/c42a196) Prepare for merging branch "training-july-2016" into the "master" branch.
* [40a611d](https://github.com/uzh/gc3pie/commit/40a611d) Replace image of "Lena" with photo of a butterfly.
* [b4a3074](https://github.com/uzh/gc3pie/commit/b4a3074) Revise part02 of the "workflows" programmers' tutorial.
* [67e30e9](https://github.com/uzh/gc3pie/commit/67e30e9) part01: Add diagrams showing how GC3Pie operates.
* [28d1a19](https://github.com/uzh/gc3pie/commit/28d1a19) Replace Gmane link with Google groups web forum.
* [dd37c5a](https://github.com/uzh/gc3pie/commit/dd37c5a) Fix some "missing toctree" errors during Sphinx compilation.
* [1f76d4c](https://github.com/uzh/gc3pie/commit/1f76d4c) Remove auto-generated UML diagrams from the docs.
* [ee0363d](https://github.com/uzh/gc3pie/commit/ee0363d) Make VM info caching optional in `VMPool.add_vm()`
* [64e3455](https://github.com/uzh/gc3pie/commit/64e3455) Start reorganizing the "configuration" section in the docs.
* [1d9a5d4](https://github.com/uzh/gc3pie/commit/1d9a5d4) Fix test `test_open_failure_unauthorized`.
* [44e444f](https://github.com/uzh/gc3pie/commit/44e444f) Handle nested sequential task collections upon `redo()`
* [fb709c3](https://github.com/uzh/gc3pie/commit/fb709c3) Remove more Grid-proxy leftover code.
* [3d92ae5](https://github.com/uzh/gc3pie/commit/3d92ae5) Fix bug that prevented redo() of nested task collections
* [b648124](https://github.com/uzh/gc3pie/commit/b648124) Sergio: *NOT WORKING* draft vbersion of gtraclong. Mainly to allow Franz to get started.
* [95e89fa](https://github.com/uzh/gc3pie/commit/95e89fa) Sergio: changed README.
* [9ecfd81](https://github.com/uzh/gc3pie/commit/9ecfd81) Add test for FAILED jobs in SLURM.
* [3fa2a13](https://github.com/uzh/gc3pie/commit/3fa2a13) Update "GC3Pie tools" slides.
* [7d54765](https://github.com/uzh/gc3pie/commit/7d54765) Sergio: allow to bundle repetition executions.
* [25161bb](https://github.com/uzh/gc3pie/commit/25161bb) Sergio:   * added support for multiple input .csv files   * fixed wrong csv chiunking (removed unecessary header)   * updated documentation
* [0ba8af5](https://github.com/uzh/gc3pie/commit/0ba8af5) Sergio: updated documentation
* [2608b5d](https://github.com/uzh/gc3pie/commit/2608b5d) Sergio: fix bug in chinking input .csv file. Reviewd documentation.
* [f421dee](https://github.com/uzh/gc3pie/commit/f421dee) Application.sbatch(): Ensure argument to `--mem` is always an integer.
* [0024761](https://github.com/uzh/gc3pie/commit/0024761) Fix "IndexError: string index out of range" in `SlurmLrms._parse_memspec()`.
* [af70eff](https://github.com/uzh/gc3pie/commit/af70eff) Sergio: added documentation on gpartialequilibrium.
* [284b850](https://github.com/uzh/gc3pie/commit/284b850) Sergio: first working version of gpartialequilibrium
* [93ce250](https://github.com/uzh/gc3pie/commit/93ce250) Fix documentation for option `-a` of `install.py`
* [92e7d3f](https://github.com/uzh/gc3pie/commit/92e7d3f) Add OpenSSL include files to the list of required OS packages.
* [8db33ea](https://github.com/uzh/gc3pie/commit/8db33ea) Bump `setuptools` requirement to 21.0.0
* [2dbd35e](https://github.com/uzh/gc3pie/commit/2dbd35e) `python-daemon` and `PyYAML` are actually needed for bare GC3Pie now...
* [5272d82](https://github.com/uzh/gc3pie/commit/5272d82) Use conditional dependency suggested by the `wheel` package.
* [52e4248](https://github.com/uzh/gc3pie/commit/52e4248) New script `gmphili.py` to run M. Philip's R code.
* [310efa0](https://github.com/uzh/gc3pie/commit/310efa0) Document issue with `pip`<8.1.2 in troubleshooting doc.
* [0cc5beb](https://github.com/uzh/gc3pie/commit/0cc5beb) Reword the ``[auth/*]`` section explanation in the config file reference.
* [c1a0c28](https://github.com/uzh/gc3pie/commit/c1a0c28) Add top-level link to the NEWS file.
* [6ecbb0c](https://github.com/uzh/gc3pie/commit/6ecbb0c) Remove unused test script for Jenkins.
* [7d5b323](https://github.com/uzh/gc3pie/commit/7d5b323) Sergio: first working version of gpredict_PopContCC https://www.s3it.uzh.ch/help/issue2766
* [9746789](https://github.com/uzh/gc3pie/commit/9746789) Sergio: first working version of gpredict_PopContCC for imls usecase.
* [7f0a254](https://github.com/uzh/gc3pie/commit/7f0a254) Sergio: docs for gthechemostat.
* [e496d1f](https://github.com/uzh/gc3pie/commit/e496d1f) Sergio: initial draft version of gsinusrange.
* [3516d37](https://github.com/uzh/gc3pie/commit/3516d37) Update `*_instance_id` attributes to `_lrms_vm_id` on load.
* [a1e7893](https://github.com/uzh/gc3pie/commit/a1e7893) New hook `Store._update_to_latest_schema()` to update objects to reflect core schema changes.
* [7ace646](https://github.com/uzh/gc3pie/commit/7ace646) Modernize some old Py2.4-style code.
* [74bb7e0](https://github.com/uzh/gc3pie/commit/74bb7e0) Fix "AttributeError: 'Run' object has no attribute 'os_instance_id'"
* [4228603](https://github.com/uzh/gc3pie/commit/4228603) Also need to upgrade `pip` to get Travis to correctly install GC3Pie.
* [5ad00ee](https://github.com/uzh/gc3pie/commit/5ad00ee) Try again to update `setuptool` on Travis
* [548382b](https://github.com/uzh/gc3pie/commit/548382b) Setup Travis build environment with latest `setuptools`
* [dafaa80](https://github.com/uzh/gc3pie/commit/dafaa80) Pin down OpenStack package versions for Python 2.6
* [eb0e928](https://github.com/uzh/gc3pie/commit/eb0e928) Cosmetic changes.
* [0c84a99](https://github.com/uzh/gc3pie/commit/0c84a99) Require `setuptools>=20.10.0`
* [3f9adea](https://github.com/uzh/gc3pie/commit/3f9adea) Update `ez_setup.py` to latest version.
* [f4f0ef9](https://github.com/uzh/gc3pie/commit/f4f0ef9) gc3libs/config.py: Minor, mostly stylistic, changes.
* [c4fdcc7](https://github.com/uzh/gc3pie/commit/c4fdcc7) Really fix issue #564 and add tests to prevent regression.
* [2568dd8](https://github.com/uzh/gc3pie/commit/2568dd8) Remove documentation about Grid-related authenticators.
* [5b613cd](https://github.com/uzh/gc3pie/commit/5b613cd) Rename test helper `temporary_config()` to `temporary_config_file()`.
* [a2a32d7](https://github.com/uzh/gc3pie/commit/a2a32d7) Rework `test_openstack.py` to use the `testing.helpers` utilities.
* [a42d057](https://github.com/uzh/gc3pie/commit/a42d057) New sub-package `gc3libs.testing` to hold utilities for writing unit tests.
* [3e056b6](https://github.com/uzh/gc3pie/commit/3e056b6) New `MinusInfinity` singleton object.
* [db99b0d](https://github.com/uzh/gc3pie/commit/db99b0d) gsPhenotypicalHomologyExample: Fix parsing month range argument.
* [55d6eed](https://github.com/uzh/gc3pie/commit/55d6eed) Sergio: fix bug in check Matlab function file
* [3617023](https://github.com/uzh/gc3pie/commit/3617023) Sergio: added option to specify Matlab function name
* [b43ad26](https://github.com/uzh/gc3pie/commit/b43ad26) Do not set extra attributes to Application object, use Run object instead.
* [7fbcc93](https://github.com/uzh/gc3pie/commit/7fbcc93) gstat: do not update jobs by default
* [19a7ae9](https://github.com/uzh/gc3pie/commit/19a7ae9) Fix issue #564
* [b6125eb](https://github.com/uzh/gc3pie/commit/b6125eb) Fix issue in gcloud list when an image has been deleted
* [cdd1404](https://github.com/uzh/gc3pie/commit/cdd1404) Fix issue in OpenStackLrms._create_instance when called without instance_type
* [5cdf924](https://github.com/uzh/gc3pie/commit/5cdf924) Sergio: working version of gthechemostat
* [c7c9be7](https://github.com/uzh/gc3pie/commit/c7c9be7) Sergio: adapter documentation
* [2809037](https://github.com/uzh/gc3pie/commit/2809037) Sergio: working version of gtrac
* [733e002](https://github.com/uzh/gc3pie/commit/733e002) Sergio: correct wrong comit from: 0844e75392dafb55debffe0535fd5c463f166253
* [4c96079](https://github.com/uzh/gc3pie/commit/4c96079) Remove *.pyc files from git repository
* [4b15848](https://github.com/uzh/gc3pie/commit/4b15848) Add comment on kernel parameters that should be checked when using INotifyPoller
* [76d2781](https://github.com/uzh/gc3pie/commit/76d2781) Update index with the last slides of the "workflow" course.
* [18438ae](https://github.com/uzh/gc3pie/commit/18438ae) Remove leftover `lena.jpg` file.
* [8177987](https://github.com/uzh/gc3pie/commit/8177987) Sergio: fix bug in 'outputs'
* [666d0a3](https://github.com/uzh/gc3pie/commit/666d0a3) Sergio: pass only output fodler name - and not a path.
* [4c5474a](https://github.com/uzh/gc3pie/commit/4c5474a) Sergio and Darren: integrated parse_events function. WORK-IN-PROGRESS
* [a7c7462](https://github.com/uzh/gc3pie/commit/a7c7462) Sergio: update in documentation
* [4fd8e66](https://github.com/uzh/gc3pie/commit/4fd8e66) Sergio: Added simple readme
* [671648d](https://github.com/uzh/gc3pie/commit/671648d) Sergio: added Step2 and Step3 to gtrac_wrapper. WORK-IN-PROGRESS.
* [754635f](https://github.com/uzh/gc3pie/commit/754635f) New part 10: advanced workflow constructs
* [b5ba47b](https://github.com/uzh/gc3pie/commit/b5ba47b) New part 10 on DependentTaskCollections
* [76b755b](https://github.com/uzh/gc3pie/commit/76b755b) part09: Fix error in title.
* [0044a76](https://github.com/uzh/gc3pie/commit/0044a76) Add parts 8 and 9
* [e5f1535](https://github.com/uzh/gc3pie/commit/e5f1535) part06: Fix typo.
* [aca2a58](https://github.com/uzh/gc3pie/commit/aca2a58) part04: Fix BLAST invocation.
* [26dd3db](https://github.com/uzh/gc3pie/commit/26dd3db) New part 07
* [f908fa8](https://github.com/uzh/gc3pie/commit/f908fa8) Finalize part 06
* [70742df](https://github.com/uzh/gc3pie/commit/70742df) New part05 on application requirements.
* [4087a78](https://github.com/uzh/gc3pie/commit/4087a78) part06: Update numbering of exercises.
* [1bf230a](https://github.com/uzh/gc3pie/commit/1bf230a) Rename part05 to part06.
* [1b3c4b8](https://github.com/uzh/gc3pie/commit/1b3c4b8) part05: Make Exercise 5.A more interesting. (Hopefully!)
* [081a2f0](https://github.com/uzh/gc3pie/commit/081a2f0) part04: Fix exercise numbering.
* [316a0df](https://github.com/uzh/gc3pie/commit/316a0df) Add README file to FAA downloads directory.
* [0c3e8f0](https://github.com/uzh/gc3pie/commit/0c3e8f0) Solutions of exercises through part 4.
* [96c2f2d](https://github.com/uzh/gc3pie/commit/96c2f2d) Update documentation index with latest slides.
* [f7e1448](https://github.com/uzh/gc3pie/commit/f7e1448) Swap parts 4 and 5
* [e25e468](https://github.com/uzh/gc3pie/commit/e25e468) part03: Add final slide on manual cleanup.
* [0844e75](https://github.com/uzh/gc3pie/commit/0844e75) Sergio: explain that UNKNOWN state could transition wihtout manual intervention.
* [92a49c4](https://github.com/uzh/gc3pie/commit/92a49c4) part04: Split exercise 4.B in two parts.
* [2d32ddb](https://github.com/uzh/gc3pie/commit/2d32ddb) part04, part05: Fix typos and other cosmetic changes.
* [104ec11](https://github.com/uzh/gc3pie/commit/104ec11) Sergio: added UNKNOWN state to list of possible Run.state
* [aa9a7bf](https://github.com/uzh/gc3pie/commit/aa9a7bf) Add part05 with support files.
* [bd24617](https://github.com/uzh/gc3pie/commit/bd24617) Collect all working examples etc. into `downloads/` directory.
* [92fced3](https://github.com/uzh/gc3pie/commit/92fced3) Finalize part04
* [d95cba9](https://github.com/uzh/gc3pie/commit/d95cba9) part02: Use `$` prompt to mark *all* shell examples.
* [636a3ec](https://github.com/uzh/gc3pie/commit/636a3ec) ex2c: Use separate output directory per task.
* [cccbcf4](https://github.com/uzh/gc3pie/commit/cccbcf4) Work around issue #559 in training material.
* [c4231a9](https://github.com/uzh/gc3pie/commit/c4231a9) part02: Fix download links
* [5443cf9](https://github.com/uzh/gc3pie/commit/5443cf9) part01: Fix workflow image name
* [27df315](https://github.com/uzh/gc3pie/commit/27df315) part00: update links with tinurl-ed ones
* [6ad4e17](https://github.com/uzh/gc3pie/commit/6ad4e17) Add solution file for ex. 2.C
* [815884f](https://github.com/uzh/gc3pie/commit/815884f) New part 3 covering useful debugging commands.
* [b5ea30e](https://github.com/uzh/gc3pie/commit/b5ea30e) part02: Add section on resources.
* [ee242d8](https://github.com/uzh/gc3pie/commit/ee242d8) gc3.sty: Add new verbatim environment `stdout`.
* [50b3f7b](https://github.com/uzh/gc3pie/commit/50b3f7b) solutions/ex2b.py: Correct script name for import.
* [a6acf7e](https://github.com/uzh/gc3pie/commit/a6acf7e) Sort `ginfo` output.
* [2a39a72](https://github.com/uzh/gc3pie/commit/2a39a72) Split old part 2 into two separate slide decks.
* [659dfbf](https://github.com/uzh/gc3pie/commit/659dfbf) Make code in the "GrayscaleApp" consistent across slides.
* [589f3b8](https://github.com/uzh/gc3pie/commit/589f3b8) Rename `day1.py` to `ex2a.py`.
* [9d533d0](https://github.com/uzh/gc3pie/commit/9d533d0) Draft slide decks for parts 1 and 2.
* [0397361](https://github.com/uzh/gc3pie/commit/0397361) Sergio: change jobname using '_'
* [e7469f5](https://github.com/uzh/gc3pie/commit/e7469f5) Sergio: first working version of gepecell
* [c5f1610](https://github.com/uzh/gc3pie/commit/c5f1610) Fix docstring of `SessionBasedScript.new_tasks()`
* [26ed36b](https://github.com/uzh/gc3pie/commit/26ed36b) First draft slide deck for the July 2016 course.
* [012fc64](https://github.com/uzh/gc3pie/commit/012fc64) Fix formatting of state transition table in `Run.state` docstring.
* [e57bbd9](https://github.com/uzh/gc3pie/commit/e57bbd9) Move existing GC3Pie course slides into `bottom-up/` directory.
* [306d170](https://github.com/uzh/gc3pie/commit/306d170) Cosmetic changes on the "programmers documentation" page
* [75df4a2](https://github.com/uzh/gc3pie/commit/75df4a2) Rename "developer documentation" to "contributor documentation".
* [18e5263](https://github.com/uzh/gc3pie/commit/18e5263) Update copyright notice in docs.
* [d5b61b1](https://github.com/uzh/gc3pie/commit/d5b61b1) Remove mention of SVN from the release banner in documentation.
* [523666d](https://github.com/uzh/gc3pie/commit/523666d) Link to `gc3pie.readthedocs.io` instead of `.org`.
* [82e34ff](https://github.com/uzh/gc3pie/commit/82e34ff) Sergio: first working version on gtrac
* [c9d6b44](https://github.com/uzh/gc3pie/commit/c9d6b44) Amend `wget` invocation to avoid potential security issues.
* [80a26e0](https://github.com/uzh/gc3pie/commit/80a26e0) Sergio: removed ref to basename when building output path
* [6c75373](https://github.com/uzh/gc3pie/commit/6c75373) examples/warholize.py: Remove `/NAME/` from output directory path.
* [873434c](https://github.com/uzh/gc3pie/commit/873434c) examples/warholize.py: Remove copy+paste error.
* [b692406](https://github.com/uzh/gc3pie/commit/b692406) examples/warholize.py: Honor the `-o`/`--output-directory` option.
* [c0712a9](https://github.com/uzh/gc3pie/commit/c0712a9) examples/warholize.py: Fix re-running of `ApplicationWithCachedResults`.
* [2862e85](https://github.com/uzh/gc3pie/commit/2862e85) Delete the `docs/html` hierarchy - we have RTD.org
* [65f8c39](https://github.com/uzh/gc3pie/commit/65f8c39) Move "GC3Pie tools training" slides into the `docs/users` hierarchy
* [108ab42](https://github.com/uzh/gc3pie/commit/108ab42) Ansible playbook to prepare the VM used for training sessions
* [989a5c8](https://github.com/uzh/gc3pie/commit/989a5c8) Update "GC3Pie tools slides" for the training on 2016-07-04
* [7781d22](https://github.com/uzh/gc3pie/commit/7781d22) Fix printing of "invalid session name" message.
* [2ae8229](https://github.com/uzh/gc3pie/commit/2ae8229) Warn that the default value for `Engine.forget_terminated` will change in future releases.
* [6254a3d](https://github.com/uzh/gc3pie/commit/6254a3d) New parameter `forget_terminated` in the `Engine` constructor.
* [88c0136](https://github.com/uzh/gc3pie/commit/88c0136) Remove useless debug logs for no-op auth checks.
* [1bc018a](https://github.com/uzh/gc3pie/commit/1bc018a) More explicit message about missing OpenStack auth parameters.
* [04b493d](https://github.com/uzh/gc3pie/commit/04b493d) Require that `os_auth_url` is defined for accessing OpenStack.
* [aa33139](https://github.com/uzh/gc3pie/commit/aa33139) Fix issue when deciding if it's the case to create a new BemoviWorkflow or not
* [4ade87a](https://github.com/uzh/gc3pie/commit/4ade87a) Properly add recursive watches when a directory tree is created
* [0217e7d](https://github.com/uzh/gc3pie/commit/0217e7d) Only merge datafiles from successfully completed jobs
* [dc40637](https://github.com/uzh/gc3pie/commit/dc40637) Fix issue with default value for inbox
* [6a09bd4](https://github.com/uzh/gc3pie/commit/6a09bd4) Fix reading of macosx-generated gbemovi csv files
* [d8e4188](https://github.com/uzh/gc3pie/commit/d8e4188) Make all `.redo()` methods accept arbitrary args.
* [39f2021](https://github.com/uzh/gc3pie/commit/39f2021) Update documentation
* [846d027](https://github.com/uzh/gc3pie/commit/846d027) shellcmd: kill a job if it exceeded max_walltime or job.requested_walltime
* [599f012](https://github.com/uzh/gc3pie/commit/599f012) gbemovi: Correctly re-read gbemovi.csv after resubmission
* [63fa4d4](https://github.com/uzh/gc3pie/commit/63fa4d4) gbemovi: Cosmetic changes
* [b262ac0](https://github.com/uzh/gc3pie/commit/b262ac0) gbemovi: avoid duplicate recipients
* [d1dd353](https://github.com/uzh/gc3pie/commit/d1dd353) GBemovi: implement cleanup XML-RPC method
* [9038203](https://github.com/uzh/gc3pie/commit/9038203) SessionBasedDaemon.list_jobs: Print also returncode
* [4db6687](https://github.com/uzh/gc3pie/commit/4db6687) gbemovi: send email notification when a video file fails to process
* [10ad229](https://github.com/uzh/gc3pie/commit/10ad229) gbemovi: Fix Merger application
* [3bec1f1](https://github.com/uzh/gc3pie/commit/3bec1f1) INotifyPoller: also watch subdirectories when recurse=True
* [cc5cae2](https://github.com/uzh/gc3pie/commit/cc5cae2) Bugfixing
* [2d88432](https://github.com/uzh/gc3pie/commit/2d88432) Move yaml dependency to requirements.daemon.txt
* [a98f328](https://github.com/uzh/gc3pie/commit/a98f328) Move downloader.py script in gc3libs/etc/
* [7882b00](https://github.com/uzh/gc3pie/commit/7882b00) Implement json_show command for SessionBasedDaemon XML-RPC interface
* [255d311](https://github.com/uzh/gc3pie/commit/255d311) Fix kill() and redo() in SequentialTaskCollection
* [2b43a2e](https://github.com/uzh/gc3pie/commit/2b43a2e) When killing a SequentialTaskCollection, kill all of its jobs
* [0ebd1e5](https://github.com/uzh/gc3pie/commit/0ebd1e5) Fix missing import of pyyaml module in tox environment
* [744b256](https://github.com/uzh/gc3pie/commit/744b256) Test helper: allow passing arguments to temporary_core()
* [5c59895](https://github.com/uzh/gc3pie/commit/5c59895) Format an History object in a yaml-friendly way
* [653a8b8](https://github.com/uzh/gc3pie/commit/653a8b8) Add `json_list` method to xml-rpc interface of SessionBasedDaemon
* [fedba55](https://github.com/uzh/gc3pie/commit/fedba55) Test that `Engine.redo()` is working on different Task and TaskCollection types.
* [7a181ad](https://github.com/uzh/gc3pie/commit/7a181ad) Add option to create a session with an existing `Store` object.
* [9ca08dd](https://github.com/uzh/gc3pie/commit/9ca08dd) Add `python-daemon` to the Tox environment used for testing.
* [3ff4969](https://github.com/uzh/gc3pie/commit/3ff4969) Record store URL in `self.url` on all `gc3libs.persistence.store.Store` subclasses.
* [a13ccb1](https://github.com/uzh/gc3pie/commit/a13ccb1) Add `inotifyx` to the Tox environment used for testing.
* [bf8d924](https://github.com/uzh/gc3pie/commit/bf8d924) Update list of supported DB schemes to what SQLAlchemy 1.1 core supports.
* [4830fe6](https://github.com/uzh/gc3pie/commit/4830fe6) Only use URI scheme up to the first `+` when deciding on a store backend.
* [aa173dd](https://github.com/uzh/gc3pie/commit/aa173dd) Use URI scheme `postgresql` for connecting to PostgreSQL DBs.
* [21a41ae](https://github.com/uzh/gc3pie/commit/21a41ae) Fix redo method of Task
* [031f4a1](https://github.com/uzh/gc3pie/commit/031f4a1) gfdiv: Allow repeating sets of arguments to form a composite session.
* [b0587ad](https://github.com/uzh/gc3pie/commit/b0587ad) Support both swift and swifts URL scheme to identify if the keystone endpoint uses SSL or not
* [db45e3f](https://github.com/uzh/gc3pie/commit/db45e3f) downloader.py: show transfer speed
* [9411406](https://github.com/uzh/gc3pie/commit/9411406) downloader.py: print transfer duration
* [7572e6e](https://github.com/uzh/gc3pie/commit/7572e6e) Fix syntax in wrapper_script
* [2593ad1](https://github.com/uzh/gc3pie/commit/2593ad1) OpenstackLrms: Support swift/http/https input files
* [032c75a](https://github.com/uzh/gc3pie/commit/032c75a) Also support uploading of output files
* [857907f](https://github.com/uzh/gc3pie/commit/857907f) Support upload of output files
* [277e329](https://github.com/uzh/gc3pie/commit/277e329) Fix creation of Url from existing Url
* [a282621](https://github.com/uzh/gc3pie/commit/a282621) Add some more logging to downloader.py
* [f04ee8d](https://github.com/uzh/gc3pie/commit/f04ee8d) FilePoller: create polling directory if it doesn't exist
* [873e228](https://github.com/uzh/gc3pie/commit/873e228) Ensure daemon is running before creating the file.
* [981545a](https://github.com/uzh/gc3pie/commit/981545a) Fix tests
* [611f1ee](https://github.com/uzh/gc3pie/commit/611f1ee) Make sure self.params.inbox contain URLs
* [52938fa](https://github.com/uzh/gc3pie/commit/52938fa) Update simpledaemon so that it also work with swift URLs
* [43d5930](https://github.com/uzh/gc3pie/commit/43d5930) ShellcmdLrms now supports also HTTP(s) and swift URLs
* [e12ffc6](https://github.com/uzh/gc3pie/commit/e12ffc6) Fix bug in Url
* [b332a42](https://github.com/uzh/gc3pie/commit/b332a42) Add SwiftPoller
* [59c5df8](https://github.com/uzh/gc3pie/commit/59c5df8) Fix Url.adjoin() for Url with a query attribute
* [a40ef7b](https://github.com/uzh/gc3pie/commit/a40ef7b) Url class now supports `query` attribute
* [c357836](https://github.com/uzh/gc3pie/commit/c357836) Properly call the pollers with the desired mask
* [1e10c71](https://github.com/uzh/gc3pie/commit/1e10c71) SessionBasedDaemon now uses a list generic 'Poller' to poll the status of multiple URLs
* [b39e1de](https://github.com/uzh/gc3pie/commit/b39e1de) Fix issue with ShellCmdLrms when no pid is found.
* [2a35e15](https://github.com/uzh/gc3pie/commit/2a35e15) Properly handle resubmit and show (SessionBasedDaemon XML-RPC interface)
* [4d420a1](https://github.com/uzh/gc3pie/commit/4d420a1) Fix IJ.path: new version of bemovi assumes it's a directory
* [ff2ec45](https://github.com/uzh/gc3pie/commit/ff2ec45) Fix kill() broken in shellcmd backend
* [902d73b](https://github.com/uzh/gc3pie/commit/902d73b) Add check for `Persistable.__eq__` method
* [c226713](https://github.com/uzh/gc3pie/commit/c226713) Add 'remove' command to SessionBasedDaemon
* [fa0666f](https://github.com/uzh/gc3pie/commit/fa0666f) Fix missing import in gbemovi.py
* [3c82fde](https://github.com/uzh/gc3pie/commit/3c82fde) Use `persistent_id` attribute when comparing two Persistable
* [f452eac](https://github.com/uzh/gc3pie/commit/f452eac) SessionBasedDaemon: add handler to root logger
* [35fe067](https://github.com/uzh/gc3pie/commit/35fe067) Add description for --threshold2
* [0d12f5f](https://github.com/uzh/gc3pie/commit/0d12f5f) Add description for the bemovi parameters
* [8a0fd62](https://github.com/uzh/gc3pie/commit/8a0fd62) Fix error in getting video parameters from csv file
* [fb818bf](https://github.com/uzh/gc3pie/commit/fb818bf) Save video parameters as Application attributes too
* [cbbe607](https://github.com/uzh/gc3pie/commit/cbbe607) Fix syntax error
* [9559393](https://github.com/uzh/gc3pie/commit/9559393) Fix syntax error
* [e66c3fd](https://github.com/uzh/gc3pie/commit/e66c3fd) Fix syntax error
* [f4e51ac](https://github.com/uzh/gc3pie/commit/f4e51ac) Ignoring files beginning with `._`. Also ensure no duplicate files are added.
* [c564a70](https://github.com/uzh/gc3pie/commit/c564a70) Add default for bemovi parameters when running gbemovi server --help
* [b6ad8a4](https://github.com/uzh/gc3pie/commit/b6ad8a4) GBemovi now also reads video parameters from csv file
* [9fe3f84](https://github.com/uzh/gc3pie/commit/9fe3f84) Rename output directory of Merger application
* [7d6b35e](https://github.com/uzh/gc3pie/commit/7d6b35e) Update "GC3Pie tools training" slides.
* [ae79df6](https://github.com/uzh/gc3pie/commit/ae79df6) .travis.yml: Install GC3Pie with the "daemon" optional component
* [5e252dc](https://github.com/uzh/gc3pie/commit/5e252dc) Make dependency on `inotifyx` optional.
* [4055bf2](https://github.com/uzh/gc3pie/commit/4055bf2) EC2Lrms: Explictly import the Crypto-submodules we're using.
* [8ea33d6](https://github.com/uzh/gc3pie/commit/8ea33d6) Allow re-doing partially terminated task collections.
* [24c425b](https://github.com/uzh/gc3pie/commit/24c425b) Many bugfixes, and implement the `merge` command
* [444c324](https://github.com/uzh/gc3pie/commit/444c324) Create the comm daemon outside its thread
* [e9ff4d9](https://github.com/uzh/gc3pie/commit/e9ff4d9) Add new video found in the input dir if they were not processed yet
* [9b53846](https://github.com/uzh/gc3pie/commit/9b53846) Support parsing of per-inbox config file
* [e4de9a1](https://github.com/uzh/gc3pie/commit/e4de9a1) Fix error when calling inotifyx.add_watch()
* [59b7e2c](https://github.com/uzh/gc3pie/commit/59b7e2c) Print full command line executed
* [7cd8932](https://github.com/uzh/gc3pie/commit/7cd8932) Cosmetic changes
* [5e911d2](https://github.com/uzh/gc3pie/commit/5e911d2) Cosmetic changes
* [261ab73](https://github.com/uzh/gc3pie/commit/261ab73) Save subparsers in SessionBasedDaemon class
* [7afedca](https://github.com/uzh/gc3pie/commit/7afedca) cosmetic changes
* [40ac02b](https://github.com/uzh/gc3pie/commit/40ac02b) Changes on the way output and input directories are dealt with
* [f06b634](https://github.com/uzh/gc3pie/commit/f06b634) Add inotify watches recursively
* [c392e9c](https://github.com/uzh/gc3pie/commit/c392e9c) Prevent harmless warning when a script dies before setting up the communication daemon
* [bb54255](https://github.com/uzh/gc3pie/commit/bb54255) Fix issue with working directory appearing twice in the
* [90f1e53](https://github.com/uzh/gc3pie/commit/90f1e53) Fix missing call to parse_args()
* [7aa5a8f](https://github.com/uzh/gc3pie/commit/7aa5a8f) Make --connect option work also with daemon working directory
* [df504cf](https://github.com/uzh/gc3pie/commit/df504cf) Implement subparsers for SessionBasedDaemon scripts
* [2786e58](https://github.com/uzh/gc3pie/commit/2786e58) Allow to change some parameters in gbemovi
* [b617ebb](https://github.com/uzh/gc3pie/commit/b617ebb) Fix error in `kill` command
* [cd74f0b](https://github.com/uzh/gc3pie/commit/cd74f0b) Various fixes and improvements
* [7373a9d](https://github.com/uzh/gc3pie/commit/7373a9d) Implement kill and resubmit (note: they don't currently work)
* [be06a27](https://github.com/uzh/gc3pie/commit/be06a27) gfdiv: More sophisticated command-line to cater to small variations in usage.
* [ea050f6](https://github.com/uzh/gc3pie/commit/ea050f6) Use Engine.stats() instead of re-computing job statistics
* [b972edc](https://github.com/uzh/gc3pie/commit/b972edc) Fix issue in _CommDaemon.print_app_table
* [47a76cd](https://github.com/uzh/gc3pie/commit/47a76cd) Add first version of GBemovi
* [7cdf9f9](https://github.com/uzh/gc3pie/commit/7cdf9f9) Also save the `IP` the daemon is listening to
* [e8b6fd4](https://github.com/uzh/gc3pie/commit/e8b6fd4) Fix issue when running client without commands
* [ef18d99](https://github.com/uzh/gc3pie/commit/ef18d99) Cosmetic changes
* [590843d](https://github.com/uzh/gc3pie/commit/590843d) --comm option is now called --listen and wants an IP.
* [d20f13e](https://github.com/uzh/gc3pie/commit/d20f13e) Name of file containing the listening port is now an attribute of _CommDaemon
* [0bfc36f](https://github.com/uzh/gc3pie/commit/0bfc36f) Always start the XML-RPC thread.
* [44ddeda](https://github.com/uzh/gc3pie/commit/44ddeda) Using a static method instead of a local function
* [2a96401](https://github.com/uzh/gc3pie/commit/2a96401) Fix `TypeError` when --output-dir is empty
* [4f6ea3f](https://github.com/uzh/gc3pie/commit/4f6ea3f) Avoid duplicated code.
* [c1cab50](https://github.com/uzh/gc3pie/commit/c1cab50) Cosmetic changes
* [5b7768c](https://github.com/uzh/gc3pie/commit/5b7768c) Specify inotifyx event mask plus cosmetic changes
* [802e18e](https://github.com/uzh/gc3pie/commit/802e18e) Removed unused scripts
* [57a7339](https://github.com/uzh/gc3pie/commit/57a7339) Skip some tests that are currently failing
* [2892327](https://github.com/uzh/gc3pie/commit/2892327) Ensure `time` is installed in the Travis container environment.
* [71e5169](https://github.com/uzh/gc3pie/commit/71e5169) Try to fix some errors while cleaning the test directory
* [209c5fe](https://github.com/uzh/gc3pie/commit/209c5fe) Add dependency on inotifyx on the proper file
* [3cd583d](https://github.com/uzh/gc3pie/commit/3cd583d) Fix issue with PIDLockFile not being able to recover the PID of the current process
* [2ae2127](https://github.com/uzh/gc3pie/commit/2ae2127) Use bundled lockfile module instead of the external one
* [d28a918](https://github.com/uzh/gc3pie/commit/d28a918) New implementation of daemon service using XML-RPC, with client
* [43a7dee](https://github.com/uzh/gc3pie/commit/43a7dee) Fix import error (script has been renamed)
* [d52aa48](https://github.com/uzh/gc3pie/commit/d52aa48) Add scripts to run BEAST using new SessionBasedDaemon
* [d78240d](https://github.com/uzh/gc3pie/commit/d78240d) Try to better handle termination of communication thread
* [c873383](https://github.com/uzh/gc3pie/commit/c873383) Log standard output and error to working directory, and check if the pidfile is present.
* [0de8644](https://github.com/uzh/gc3pie/commit/0de8644) Improve the chances we terminate cleanly
* [a6ebbc2](https://github.com/uzh/gc3pie/commit/a6ebbc2) Removed testing function
* [bc69eed](https://github.com/uzh/gc3pie/commit/bc69eed) Print the same output as gsession -l -r when running "list -l" command via IPC
* [964e3e1](https://github.com/uzh/gc3pie/commit/964e3e1) Use nanoservice instead of oi to implement the IPC thread
* [11c4c98](https://github.com/uzh/gc3pie/commit/11c4c98) Add lockfile as a requirement
* [7105b0a](https://github.com/uzh/gc3pie/commit/7105b0a) Implement simple IPC mechanism to interact with SessionBasedDaemon
* [42d3644](https://github.com/uzh/gc3pie/commit/42d3644) Save session at every loop, so that one can inspect the current session
* [d069fda](https://github.com/uzh/gc3pie/commit/d069fda) Add script used to test SessionBasedDaemon
* [dbca1d1](https://github.com/uzh/gc3pie/commit/dbca1d1) Cosmetic changes
* [b664d70](https://github.com/uzh/gc3pie/commit/b664d70) Add functional test for SessionBasedDaemon class
* [898466d](https://github.com/uzh/gc3pie/commit/898466d) Create inbox directories if they don't exist
* [0ab7a66](https://github.com/uzh/gc3pie/commit/0ab7a66) Correcly handle --working-dir
* [24d100f](https://github.com/uzh/gc3pie/commit/24d100f) First working version of SessionBasedDaemon class
* [4dec8e5](https://github.com/uzh/gc3pie/commit/4dec8e5) Make test scripts NOT executable, otherwise nose will ignore them
* [8ea68bd](https://github.com/uzh/gc3pie/commit/8ea68bd) gkill: Clarify error message when killing jobs that are in NEW state.
* [6eaabac](https://github.com/uzh/gc3pie/commit/6eaabac) gfdiv: If there is no error then the task is ...well... successful!
* [b6f0dd6](https://github.com/uzh/gc3pie/commit/b6f0dd6) Application: Ensure `.stderr` equals `.stdout` when joining the two streams.
* [7cfab5a](https://github.com/uzh/gc3pie/commit/7cfab5a) gfdiv: Ensure path to STDERR is not ``None``.
* [053bf9b](https://github.com/uzh/gc3pie/commit/053bf9b) Remove `.pyc` file accidentally committed in.
* [ea5e4e7](https://github.com/uzh/gc3pie/commit/ea5e4e7) gfdiv: Do *not* force MATLAB to run single-threaded.
* [bf6cddd](https://github.com/uzh/gc3pie/commit/bf6cddd) gfdiv: Fix `TypeError`.
* [e4e117e](https://github.com/uzh/gc3pie/commit/e4e117e) Fix `test_invalid_resource_type()`.
* [5b3126e](https://github.com/uzh/gc3pie/commit/5b3126e) ShellcmdLrms: honor application environment definitions again.
* [752a715](https://github.com/uzh/gc3pie/commit/752a715) Make `Engine.submit()` honor the `resubmit` argument correctly.
* [836861f](https://github.com/uzh/gc3pie/commit/836861f) Ensure `SequentialTaskCollection` is in state RUNNING when a task is running.
* [7c4155e](https://github.com/uzh/gc3pie/commit/7c4155e) Simplify `tests/test_engine.py` with the aid of new test helpers.
* [f2090dd](https://github.com/uzh/gc3pie/commit/f2090dd) New method `Engine.redo()` to re-start execution of a task.
* [db19059](https://github.com/uzh/gc3pie/commit/db19059) Factor out common code for `Engine.add` and `Engine.remove`
* [99d588d](https://github.com/uzh/gc3pie/commit/99d588d) Move `Simple*TaskCollection` classes into module `tests/helpers.py`.
* [5e78223](https://github.com/uzh/gc3pie/commit/5e78223) New `.redo()` method to re-run a Task or a TaskCollection.
* [04b8e66](https://github.com/uzh/gc3pie/commit/04b8e66) Add tests for the `SequentialTaskCollection.stage()` method.
* [745b387](https://github.com/uzh/gc3pie/commit/745b387) Make `TaskCollection.progress()` cooperate with mix-in classes.
* [e0b3830](https://github.com/uzh/gc3pie/commit/e0b3830) Rename module `tests/utils.py` to `tests/helpers.py`.
* [be8044f](https://github.com/uzh/gc3pie/commit/be8044f) Remove `Application.fetch_output()` which shadows the like-named method `Task.fetch_output()`.
* [2b72c4e](https://github.com/uzh/gc3pie/commit/2b72c4e) Clarify that `StopOnError` and `AbortOnError` should be mixed in *before* the base task collection class.
* [2420f38](https://github.com/uzh/gc3pie/commit/2420f38) Fix docstring for `SequentialTaskCollection`.
* [414d43d](https://github.com/uzh/gc3pie/commit/414d43d) New method `_OnError.complete()` to select behavior of the mix-in `next()` method.
* [57e54dc](https://github.com/uzh/gc3pie/commit/57e54dc) SequentialTaskCollection: new method `.stage()` returns the currently-executing Task.
* [338d086](https://github.com/uzh/gc3pie/commit/338d086) Core.submit(): forward `targets` argument to Tasks.
* [2c81cf6](https://github.com/uzh/gc3pie/commit/2c81cf6) Fix syntax error introduced in last commit.
* [48bc606](https://github.com/uzh/gc3pie/commit/48bc606) ShellcmdLrms: ensure arguments are quoted.
* [a3ce461](https://github.com/uzh/gc3pie/commit/a3ce461) ShellcmdLrms: handle `?` field values in `time` output.
* [a26bdec](https://github.com/uzh/gc3pie/commit/a26bdec) Make `gridrun.py` work again.
* [fcdfd0d](https://github.com/uzh/gc3pie/commit/fcdfd0d) Small fixes to docstrings and log messages.
* [8aa925c](https://github.com/uzh/gc3pie/commit/8aa925c) SshTransport: do not even try to connect if remote host name is not set.
* [123b337](https://github.com/uzh/gc3pie/commit/123b337) Final version of the slides for the 2016-04-04 training.
* [6a3f76a](https://github.com/uzh/gc3pie/commit/6a3f76a) Draft slides for the "GC3Pie tools" training.
* [9c18100](https://github.com/uzh/gc3pie/commit/9c18100) Make help text for `gsession list` tell more clearly what it does.
* [776af0c](https://github.com/uzh/gc3pie/commit/776af0c) ShellcmdLrms: execute child program as direct child of `time`.
* [c75b188](https://github.com/uzh/gc3pie/commit/c75b188) Add draft ToC for the "GC3Pie tools" training.
* [1cc4b92](https://github.com/uzh/gc3pie/commit/1cc4b92) No need to explicitly convert to string in `logging.debug` call.
* [c10d30c](https://github.com/uzh/gc3pie/commit/c10d30c) Fix typo.
* [a318d68](https://github.com/uzh/gc3pie/commit/a318d68) SLURM: Handle case where `squeue` and `sacct` disagree about a completed jobs' state.
* [3f1e45d](https://github.com/uzh/gc3pie/commit/3f1e45d) Ensure `BatchLrms` does not fail if no "secondary accounting" command is defined but the primary accounting fails.
* [d74d333](https://github.com/uzh/gc3pie/commit/d74d333) Add tests for checking division of quantities.
* [1636e1c](https://github.com/uzh/gc3pie/commit/1636e1c) Fix division of `gc3libs.quantity.Quantity` by a number when `from __future__ import division` is in effect.
* [e1a3dff](https://github.com/uzh/gc3pie/commit/e1a3dff) Fix error message.
* [06f53f7](https://github.com/uzh/gc3pie/commit/06f53f7) gstat: New command-line option `-b` to only print summary table of states.
* [ee1db40](https://github.com/uzh/gc3pie/commit/ee1db40) examples/gdemo_session: Make it stop eventually.
* [eef71f3](https://github.com/uzh/gc3pie/commit/eef71f3) examples/gdemo_session: Cosmetic changes.
* [284c051](https://github.com/uzh/gc3pie/commit/284c051) ShellcmdLrms: Correctly set task resource usage info.
* [adca250](https://github.com/uzh/gc3pie/commit/adca250) Remove Emacs backup file commited in error.
* [f07812f](https://github.com/uzh/gc3pie/commit/f07812f) LSF: consider jobs for which `bjobs` has no information as TERMINATING.
* [d026670](https://github.com/uzh/gc3pie/commit/d026670) Change interface of `BatchSystem._parse_*_output()` to also require stderr.
* [daad83e](https://github.com/uzh/gc3pie/commit/daad83e) Fix typo.
* [69ac5a2](https://github.com/uzh/gc3pie/commit/69ac5a2) Fix syntax error in `Engine.progress()`.
* [28e5d28](https://github.com/uzh/gc3pie/commit/28e5d28) Engine.progress(): log error if update_job_state() returns an invalid state.
* [975d5f4](https://github.com/uzh/gc3pie/commit/975d5f4) gfdiv: Set maximum allowed duration to 30 days.
* [d27834b](https://github.com/uzh/gc3pie/commit/d27834b) gfdiv: Retry each task upon "out of memory" errors.
* [8138565](https://github.com/uzh/gc3pie/commit/8138565) Make the `occurs()` utility be more flexible in matching.
* [a43e7f2](https://github.com/uzh/gc3pie/commit/a43e7f2) Make `sh_quote_safe` and `sh_quote_unsafe` accept arbitrary-type arguments.
* [19ae6d5](https://github.com/uzh/gc3pie/commit/19ae6d5) More tests for the SLURM backend.
* [131a0cd](https://github.com/uzh/gc3pie/commit/131a0cd) SLURM: Correctly detect failure of *both* `squeue` and `sacct` to report on a terminated job.
* [4b376d7](https://github.com/uzh/gc3pie/commit/4b376d7) Restore `accounting_delay` functionality.
* [e61cfaa](https://github.com/uzh/gc3pie/commit/e61cfaa) test_slurm.py: Make sample-creating functions obey their parameter list.
* [9682ec4](https://github.com/uzh/gc3pie/commit/9682ec4) Fix typo in LSF module heading.
* [a6bb8f4](https://github.com/uzh/gc3pie/commit/a6bb8f4) Fix module name in doctests.
* [0b90105](https://github.com/uzh/gc3pie/commit/0b90105) Rewrite `update_job_status()` in batch-system backends.
* [5028051](https://github.com/uzh/gc3pie/commit/5028051) Add doctest for function `shellexit_to_returncode`.
* [1be5666](https://github.com/uzh/gc3pie/commit/1be5666) Test that the SLURM backend behaves correctly when the accounting command fails.
* [46715d8](https://github.com/uzh/gc3pie/commit/46715d8) More tests that the SLURM backend does the right thing with differnt command/output sequences.
* [349d74b](https://github.com/uzh/gc3pie/commit/349d74b) Provide an error message if a job state is not among those we expect.
* [0581a23](https://github.com/uzh/gc3pie/commit/0581a23) parse_range(): Accept single number as "degenerate" range.
* [7bca5a6](https://github.com/uzh/gc3pie/commit/7bca5a6) gfdiv: Radius range includes *both* ends.
* [72473da](https://github.com/uzh/gc3pie/commit/72473da) gfdiv: Force MATLAB to use a single computing thread.
* [dfc513f](https://github.com/uzh/gc3pie/commit/dfc513f) gfdiv: Require 7GB of memory to run.
* [21b0d8e](https://github.com/uzh/gc3pie/commit/21b0d8e) First draft of the `gc3apps/geo/gfdiv.py` script.
* [0e963e5](https://github.com/uzh/gc3pie/commit/0e963e5) Add doctests for `gc3libs.utils.basename_sans`.
* [8644ba2](https://github.com/uzh/gc3pie/commit/8644ba2) New `gc3libs.utils.parse_range()` function to parse `LOW:HIGH[:STEP]` expressions.
* [46b775d](https://github.com/uzh/gc3pie/commit/46b775d) Fix incorrect quoting of arguments in `gc3libs.backends.shellcmd`.
* [d057ea0](https://github.com/uzh/gc3pie/commit/d057ea0) Ensure `setup.py` is run with a "recent enough" setuptools.
* [ab09b2c](https://github.com/uzh/gc3pie/commit/ab09b2c) Update `ez_setup.py` to the latest version available.
* [b1e96d9](https://github.com/uzh/gc3pie/commit/b1e96d9) Make Travis reporting more verbose.
* [76c1e4f](https://github.com/uzh/gc3pie/commit/76c1e4f) gc3pie.conf.example: Use "Science Cloud" values for the "OpenStack" example.
* [c5d929a](https://github.com/uzh/gc3pie/commit/c5d929a) gc3pie.conf.example: Fix reference URL and other small cosmetic changes.
* [f2c0061](https://github.com/uzh/gc3pie/commit/f2c0061) Remove old workaround code for `logging` bugs in Python 2.4 and 2.5.
* [1a06b9c](https://github.com/uzh/gc3pie/commit/1a06b9c) gdemo_simple.py: Show off the new log colorization.
* [6d8474e](https://github.com/uzh/gc3pie/commit/6d8474e) Colorize log lines according to level.
* [e5b30e0](https://github.com/uzh/gc3pie/commit/e5b30e0) `Session._load_session`: use `with` statement instead of old-style try/except
* [9c8bd76](https://github.com/uzh/gc3pie/commit/9c8bd76) `Session.set_start_timestamp` and `.set_end_timestamp` now honor the `time` argument.
* [8d0a96d](https://github.com/uzh/gc3pie/commit/8d0a96d) Sergio: first working version of gqhg
* [a31c5f8](https://github.com/uzh/gc3pie/commit/a31c5f8) Sergio: forst working version - Adrian to clean it up
* [e0a3afb](https://github.com/uzh/gc3pie/commit/e0a3afb) Sergio: added readme and included option for `master script` to be executed remotely.
* [c3ffcfb](https://github.com/uzh/gc3pie/commit/c3ffcfb) Sergio: removed 'import pandas'
* [ca8f1bc](https://github.com/uzh/gc3pie/commit/ca8f1bc) Sergio: remove pandas dependencies - runs in vanilla Ubuntu
* [5d9d161](https://github.com/uzh/gc3pie/commit/5d9d161) Add `libffi` to the list of required packages.
* [1847be8](https://github.com/uzh/gc3pie/commit/1847be8) Sergio: first working version of gthermostat
* [33c7ac9](https://github.com/uzh/gc3pie/commit/33c7ac9) Sergio: changed test from int() to isinstance(...,int)
* [d8b5cd2](https://github.com/uzh/gc3pie/commit/d8b5cd2) Sergio: updated version with support for '0' as allowed value for input hunting value
* [65f7b6e](https://github.com/uzh/gc3pie/commit/65f7b6e) "Sergio: first working version of gtopology"
* [885bd4e](https://github.com/uzh/gc3pie/commit/885bd4e) Sergio: added step to activategc3pie virtual env
* [5031ee0](https://github.com/uzh/gc3pie/commit/5031ee0) Sergio: updated documentation
* [9b53ca5](https://github.com/uzh/gc3pie/commit/9b53ca5) First draft of the script needed for issue2087
* [aa11d83](https://github.com/uzh/gc3pie/commit/aa11d83) First working version of gcombi for Gaia Lombardi from business.econ.uzh, issue2155
* [d5d8a24](https://github.com/uzh/gc3pie/commit/d5d8a24) install.py: Fix "KeyError: 'anaconda_root_dir'" when installing with Anaconda Pytyhon
* [459d3dd](https://github.com/uzh/gc3pie/commit/459d3dd) install.py: Fix "NameError: global name 'loggin' is not defined" when installing with Anaconda Pytyhon
* [320d881](https://github.com/uzh/gc3pie/commit/320d881) install.py: Fix "NameError: global name 'fnmatch' is not defined" when installing with Anaconda Pytyhon
* [25b1a75](https://github.com/uzh/gc3pie/commit/25b1a75) Sergio: changed driver script to gsPhenotypicalHomologyExample.py
* [93bdc48](https://github.com/uzh/gc3pie/commit/93bdc48) Sergio: added readme for gsPhenotypicalHomologyExample usecase
* [ffe54b4](https://github.com/uzh/gc3pie/commit/ffe54b4) Sergio: changed output filename in wrapper script
* [905fcb1](https://github.com/uzh/gc3pie/commit/905fcb1) Sergio: initial working version of gsheepriver
* [0f8268b](https://github.com/uzh/gc3pie/commit/0f8268b) Sergio: updated gzip mimetype check to http://tools.ietf.org/html/rfc6713
* [ef8c5c7](https://github.com/uzh/gc3pie/commit/ef8c5c7) Sergio: fixed month range for R script
* [7d52455](https://github.com/uzh/gc3pie/commit/7d52455) Sergio: wrap execution of R script
* [ecf3f2c](https://github.com/uzh/gc3pie/commit/ecf3f2c) [gc3apps/psychology.uzh.ch/gfsurfer.py] download only specific output requested needed for later processing, [gc3libs/etc/gfsurfer_wrapper.py]: remove quality check since it will be done on the login node
* [ddfd1f3](https://github.com/uzh/gc3pie/commit/ddfd1f3) Sergio: working version of gchelsa for running RSAGA-based scripts
* [213b39d](https://github.com/uzh/gc3pie/commit/213b39d) Sergio: updated recon-all call according to specs from Franz
* [42b6370](https://github.com/uzh/gc3pie/commit/42b6370) Fix wrong missing submission of new jobs
* [368e08d](https://github.com/uzh/gc3pie/commit/368e08d) First working version of gfsurfer
* [e727408](https://github.com/uzh/gc3pie/commit/e727408) Fix attribute error, and use beast v1 by default
* [f0e04c1](https://github.com/uzh/gc3pie/commit/f0e04c1) Sergio: updated version of gnlp
* [a26acfc](https://github.com/uzh/gc3pie/commit/a26acfc) [gfsurfer]: added missing closing bracket
* [c120485](https://github.com/uzh/gc3pie/commit/c120485) Sergio: NOT WORKIGN version of gfsurfer/. To be handed over to Tyanko and Filippo
* [54f0736](https://github.com/uzh/gc3pie/commit/54f0736) Add two gc3pie scripts for IRM institute
* [530dbf4](https://github.com/uzh/gc3pie/commit/530dbf4) Sergio: working version of ggatk for running GATK pipeline.
* [f7e064c](https://github.com/uzh/gc3pie/commit/f7e064c) Do not use relative links as they don't DTRT on ReadTheDocs.org
* [7374551](https://github.com/uzh/gc3pie/commit/7374551) Fix links in the "programmers" section of the documentation.
* [a5db80c](https://github.com/uzh/gc3pie/commit/a5db80c) Sergio: updated gnift wrapper. sort timepoints.
* [f105a2c](https://github.com/uzh/gc3pie/commit/f105a2c) Sergio: **not working** changed command line specs. Added additional 'command' argument.
* [02fe962](https://github.com/uzh/gc3pie/commit/02fe962) Sergio: **NOT WORKING** in progress development of grosetta2015 for Hamed
* [0c40b1f](https://github.com/uzh/gc3pie/commit/0c40b1f) Sergio: NON WORKING!!! version of grosetta2015.py and its related script in etc/rosetta2015.sh To be continued by Hamed
* [14b06d0](https://github.com/uzh/gc3pie/commit/14b06d0) Sergio: first working version of gnift for INAPIC support.
* [4ffacd5](https://github.com/uzh/gc3pie/commit/4ffacd5) Sergio: first working version of gkjpd script
* [3bac051](https://github.com/uzh/gc3pie/commit/3bac051) Sergio: tests and small README for gsisp
* [8ba3f6a](https://github.com/uzh/gc3pie/commit/8ba3f6a) Sergio: first working version of gsisp: https://www.s3it.uzh.ch/help/issue1628
* [7766626](https://github.com/uzh/gc3pie/commit/7766626) Sergio: enabled execution in Docker environment
* [1b9bc82](https://github.com/uzh/gc3pie/commit/1b9bc82) Remove dependency of OpenStack and EC2 backends on the `gc3libs.config`.
* [f08c1a5](https://github.com/uzh/gc3pie/commit/f08c1a5) gc3libs.backends.ec2: Show complete import error message if importing `boto` fails.
* [dd9dd4d](https://github.com/uzh/gc3pie/commit/dd9dd4d) When using the OpenStack feature, require `importlib` as it's needed by recent versions of `python-novaclient`.
* [9fe55ef](https://github.com/uzh/gc3pie/commit/9fe55ef) gc3libs.backends.openstack: Show complete import error message if importing `novaclient` fails.
* [2456e40](https://github.com/uzh/gc3pie/commit/2456e40) Use `pip install -e` in `.travis.yml`
* [3bfcb79](https://github.com/uzh/gc3pie/commit/3bfcb79) First draft `.travis.yml` file.
* [c334c77](https://github.com/uzh/gc3pie/commit/c334c77) gndn.py: Fix issue #500
* [992ceed](https://github.com/uzh/gc3pie/commit/992ceed) Use logging's lazy argument interpolation, instead of `%`'s eager one.
* [9740042](https://github.com/uzh/gc3pie/commit/9740042) SLURM: ensure multi-core jobs are run as such.
* [751b413](https://github.com/uzh/gc3pie/commit/751b413) SLURM: use `--mem` instead of `--mem-per-cpu`.
* [97d5716](https://github.com/uzh/gc3pie/commit/97d5716) SLURM: remove outdated comment.
* [9997219](https://github.com/uzh/gc3pie/commit/9997219) Reformat docstring of class `Application`
* [1c19de7](https://github.com/uzh/gc3pie/commit/1c19de7) Specify that `requested_memory` is a *per job* requirement.
* [e8aee49](https://github.com/uzh/gc3pie/commit/e8aee49) `create_core` and `create_engine()` factory functions can now route keyword args to the appropriate ctor.
* [6a3def6](https://github.com/uzh/gc3pie/commit/6a3def6) tests/test_engine.py: Fix file header
* [3d82355](https://github.com/uzh/gc3pie/commit/3d82355) New module `gc3libs.tests.utils` to collect utility functions to ease writing unit tests.
* [4e3efcc](https://github.com/uzh/gc3pie/commit/4e3efcc) Remove unused `gc3libs/tests/common.py` file.
* [361784f](https://github.com/uzh/gc3pie/commit/361784f) Make `Core` into a "new-style" class.
* [7d60aba](https://github.com/uzh/gc3pie/commit/7d60aba) Fix top-level `INSTALL.rst` symlink.
* [932ff8e](https://github.com/uzh/gc3pie/commit/932ff8e) Correct list of dependencies in `tox.ini` file.
* [7c13148](https://github.com/uzh/gc3pie/commit/7c13148) Include `requirements.*.txt` files into the Python package.
* [b3a43d7](https://github.com/uzh/gc3pie/commit/b3a43d7) Use SQL Alchemy's "generic types" instead of hard-coding SQL types.
* [4303db6](https://github.com/uzh/gc3pie/commit/4303db6) Use SQL Alchemy's `LargeBinary` type instead of `BLOB`.
* [302b8c6](https://github.com/uzh/gc3pie/commit/302b8c6) Sergio: fixed typo in wrapper reference
* [43bf2c7](https://github.com/uzh/gc3pie/commit/43bf2c7) Sergio: avoid creating execution files; just use single wrapper and pass different parameters
* [fc9b39d](https://github.com/uzh/gc3pie/commit/fc9b39d) Sergio: avoid creating execution file; just use generic wrapper and define different combination of input parameters
* [d3c3001](https://github.com/uzh/gc3pie/commit/d3c3001) Sergio: bugfix: change input_dir reference to master_script
* [9623641](https://github.com/uzh/gc3pie/commit/9623641) Sergio: first working version of gparseurl
* [432d4f3](https://github.com/uzh/gc3pie/commit/432d4f3) Sergio: cleaned up useless .pyc file
* [8688c8f](https://github.com/uzh/gc3pie/commit/8688c8f) Sergio: first working version of GC3Pie workflow for UZH/Geo usecase
* [c872731](https://github.com/uzh/gc3pie/commit/c872731) setup.py: Store list of required modules into `requirements.*.txt` files.
* [a4eebe1](https://github.com/uzh/gc3pie/commit/a4eebe1) Reword docstring of `gc3libs.create_engine()`
* [259bfdb](https://github.com/uzh/gc3pie/commit/259bfdb) Reword docstring for `gc3libs.cmdline.nonnegative_int` and `positive_int`.
* [96fd585](https://github.com/uzh/gc3pie/commit/96fd585) Correct description of the `gc3libs.core` module.
* [cb45cd0](https://github.com/uzh/gc3pie/commit/cb45cd0) Remove last use of `gc3libs.utils.ifelse`.
* [9678a1a](https://github.com/uzh/gc3pie/commit/9678a1a) Remove extra ToC from publications list.
* [36f8e32](https://github.com/uzh/gc3pie/commit/36f8e32) Fix address of GC3Pie mailing-list.
* [86fb3cc](https://github.com/uzh/gc3pie/commit/86fb3cc) Add list of papers enabled by GC3Pie to the "publications".
* [57ef94b](https://github.com/uzh/gc3pie/commit/57ef94b) Migrate slides etc. over from the GoogleCode site.
* [ad78bae](https://github.com/uzh/gc3pie/commit/ad78bae) Fix typo: rename `gc3apps/phsycology.uzh.ch` to `psychology.uzh.ch`
* [9f573bd](https://github.com/uzh/gc3pie/commit/9f573bd) Rename documentation source files to end with `.rst`
* [2fc1a74](https://github.com/uzh/gc3pie/commit/2fc1a74) OpenStackLRMS: Remove unused construction argument `image_name`.
* [bd96800](https://github.com/uzh/gc3pie/commit/bd96800) Update and expand `gc3pie.conf.example`.
* [38cab53](https://github.com/uzh/gc3pie/commit/38cab53) Remove mention of ARC-based resources from example config file.
* [8197884](https://github.com/uzh/gc3pie/commit/8197884) Sergio: added option to specify name of aggregated result .csv file
* [796f853](https://github.com/uzh/gc3pie/commit/796f853) Sergio: localresource is not created if `_get_subresource` fails. Moved return statement within same try ... except block
* [47378e3](https://github.com/uzh/gc3pie/commit/47378e3) Restore top-level README file.
* [7157d86](https://github.com/uzh/gc3pie/commit/7157d86) Sergio: bugfix resulted in erroneous merging
* [7176b67](https://github.com/uzh/gc3pie/commit/7176b67) Sergio: first working version of Gspg - usecase prof. Tessone (business.uzh)
* [9de1f5e](https://github.com/uzh/gc3pie/commit/9de1f5e) Sergio: 'nics' have to be specified as part of server.create on Openstack from Kilo on.
* [e06b237](https://github.com/uzh/gc3pie/commit/e06b237) Sergio: first working version of Gsps for usecase support for Prof. Tessone (business.uzh)
* [f71dfc9](https://github.com/uzh/gc3pie/commit/f71dfc9) Sergio: 'nics' has to be specified on Openstack distribution form Kilo on
* [62bfe2b](https://github.com/uzh/gc3pie/commit/62bfe2b) Fix Issue #496
* [bfcff12](https://github.com/uzh/gc3pie/commit/bfcff12) Link to issues on GitHub, not Google Code.
* [8bed805](https://github.com/uzh/gc3pie/commit/8bed805) Fix more links after migration to GitHub
* [362cf51](https://github.com/uzh/gc3pie/commit/362cf51) Fix link to `INSTALL.rst` page so that it renders properly on GitHub
* [2b85bef](https://github.com/uzh/gc3pie/commit/2b85bef) Update installation instructions and links for GitHub
* [f0e7e3d](https://github.com/uzh/gc3pie/commit/f0e7e3d) Sergio: changed link to installation instructions on readthedocs.org
* [b058bf0](https://github.com/uzh/gc3pie/commit/b058bf0) Fix install command incantation.
* [c2a5fc4](https://github.com/uzh/gc3pie/commit/c2a5fc4) Update instructions for contributing to GC3Pie
* [db7dd8c](https://github.com/uzh/gc3pie/commit/db7dd8c) New installation script with instructions.
* [69df2b9](https://github.com/uzh/gc3pie/commit/69df2b9) Rename `README.txt` file as `README.rst` to allow GitHub to display it properly.
* [186a3a2](https://github.com/uzh/gc3pie/commit/186a3a2) Remove root directory `gc3pie`, which does no longer make sense in GitHub.

