.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc


Contributing to GC3Pie
======================

First of all, thanks for wanting to contribute to GC3Pie!  GC3Pie
is an open-ended endeavour, and we're always looking for new ideas,
suggestions, and new code.  (And also, for fixes to bugs old and new ;-))

The paragraphs below should brief you about the organization of the
GC3Pie code repositories, and the suggested guidelines for code and
documentation style.  Feel free to request more info or discuss the
existing recommendations on the `GC3Pie mailing list`_



Code repository organization
----------------------------

GC3Pie code is hosted in a GitHub_ repository, which you can
access online__ or using any Git_ client.

.. __: http://github.com/uzh/gc3pie/

We encourage anyone to fork the repository and contribute back
modifications in the form of `pull requests`_.

.. _`pull requests`: https://help.github.com/articles/using-pull-requests/

The *master* branch should always be deployable: code in *master*
should normally run without major known issues (but it may contain
code that has yet not been released to PyPI_).  A tag is created on
the *master* branch each time code is released to PyPI_.  Development
happens on separate branches (or forks) which are then merged into
*master* via `pull requests`_.

.. _pypi: http://pypi.python.org/


Repository structure
~~~~~~~~~~~~~~~~~~~~

The GC3Pie code repository has the following top-level structure;
there is one subdirectory for each of the main parts of GC3Pie:

* The ``gc3libs`` directory contains the GC3Libs code, which is the
  core of GC3Pie.  GC3Libs are extensively described in the `API
  <gc3libs>`:ref: section of this document; read the module descriptions
  to find out where your new suggested functionality would suit best.
  If unsure, ask on the `GC3Pie mailing list`_.

* The ``gc3utils`` directory contains the sources for the low-level
  GC3Utils command-line utilities.

* The ``gc3apps`` directory contains the sources for higher level
  scripts that implement some computational use case of independent
  interest.

  The ``gc3apps`` directory contains one subdirectory per *application
  script*.  Actually, each subdirectory can contain one or more Python
  scripts, as long as they form a coherent bundle; for instance,
  Rosetta_ is a suite of applications in computational biology: there
  are different GC3Apps script corresponding to different uses of the
  Rosetta_ suite, all of them grouped into the ``rosetta``
  subdirectory.

  Subdirectories of the ``gc3apps`` directory follow this naming
  convention:

  - the directory name is the main application name, if the
    application that the scripts wrap is a known, publicly-released
    computational application (e.g., Rosetta_, GAMESS_)

  - the directory name is the requestor's name, if the application
    that the scripts wrap is some research code that is being
    internally developed. For instance, the ``bf.uzh.ch`` directory
    contains scripts that wrap code for economic simulations that is
    being developed at the `Banking and Finance Institute of the University
    of Zurich`__

    .. __: http://www.bf.uzh.ch/


Package generation
------------------

Due to issue 329_, we don't use the automatic discovery *feature* of
``setuptools``, so the files included in the distributed packages are
those in the ``MANIFEST.in`` file, please check `The MANIFEST.in
template`_ section of the python documentation for a syntax
reference. We usually include only code, documentation, and related
files. We also include the regression tests, but we **do not include**
the application tests in ``gc3apps/*/test`` directories.

.. _329: https://github.com/uzh/gc3pie/issues/329
.. _`The MANIFEST.in template`: http://docs.python.org/distutils/sourcedist.html#the-manifest-in-template


Testing the code
----------------

In developing GC3Pie we try to use a `Test Driven Development`_
approach, in the light of the quote: *It's tested or it's broken*. We
use `tox`_ and `pytest`_ as test runners, which make creating tests very
easy.

.. _`Test Driven Development`: http://en.wikipedia.org/wiki/Test-driven_development
.. _`tox`: http://tox.testrun.org/latest/
.. _`pytest`: https://docs.pytest.org/en/latest/


Running the tests
~~~~~~~~~~~~~~~~~

You can both run tests on your current environment using ``pytest`` or use `tox_` to
create and run tests on separate environments. We suggest you to use
``pytest`` while you are still fixing the problem, in order to be
able to run only the failing test, but we strongly suggest you to run
``tox`` *before* committing your code.

Running tests with ``pytest``
+++++++++++++++++++++++++++++

In order to have the ``pytest`` program, you need to install `pytest_`
in your current environment **and** gc3pie must be installed in
develop mode::

    pip install pytest
    python setup.py develop

Then, from the top level directory, run the tests with::

    pytest -v

PyTest_ will then crawl the directory tree looking for available
tests. You can also specify a subset of the available sets, by:

* specifying the directory from which nose should start looking for tests::

    # Run only backend-related tests
    pytest -v gc3libs/backends/

* specifying the file containing the tests you want to run::

    # Run only tests contained in a specific file
    pytest -v gc3libs/tests/test_session.py

* specifying the id of the test (a test ID is the file name, a double
  colon, and the test function name)::

    # Run only test `test_engine_limits` in file `test_engine.py`
    pytest test_engine.py::test_engine_limits

Running multiple tests
++++++++++++++++++++++

In order to test GC3Pie against multiple version of python we use
`tox`_, which creates virtual environments for all configured python
version, runs `pytest`_ inside each one of them, and prints a summary of
the test results.

You don't need to have ``tox`` installed in the virtual environment you
use to develop gc3pie, you can create a new virtual environment and
install ``tox`` on it with.

Running tox_ is straightforward; just type ``tox`` on the command-line
in GC3Pie's top level source directory.

The default ``tox.ini`` file shipped with GC3Pie attempts to test all
Python versions from 2.4 to 2.7 (inclusive).  If you want to run tests
only for a specific version of python, for instance Python 2.6, use
the ``-e`` option::

    tox -e py26
    [...]
    Ran 118 tests in 14.168s

    OK (SKIP=9)
    __________________________________________________________ [tox summary] ___________________________________________________________
    [TOX] py26: commands succeeded
    [TOX] congratulations :)

Option ``-r`` instructs `tox`:command: to re-build the testing virtual
environment. This is usually needed when you update the dependencies
of GC3Pie or when you add or remove command line programs or
configuration files. However, if you feel that the environments can be
*unclean*, you can clean up everything by:

1) deleting all the ``*.pyc`` file in your source tree::

      find . -name '*.pyc' -delete

2) deleting and recreating tox virtual environments::

      tox -r


Writing tests
~~~~~~~~~~~~~

Please remember that it may be hard to understand, whenever a test
fails, if it's a bug in the code or in the tests!  Therefore please
remember:

* Try to keep tests as simple as possible, and *always* simpler than
  the tested code. (*Debugging is twice as hard as writing the code in
  the first place.*,  Brian W. Kernighan and P. J. Plauger)

* Write multiple independent tests to test different possible behavior
  and/or different methods of a class.

* Tests should cover methods and functions, but also specific use cases.

* If you are fixing a bug, it's good practice to write a test to check
  if the bug is still there, in order to avoid to re-include the bug
  in the future.

* Tests should clean up every temporary file they create.

Writing tests is very easy: just create a file whose name begins with
``test_``, then put in it some functions which name begins with
``test_``; the pytest_ framework will automatically call each one of
them. Moreover, pytest_ will run also any pytest_ which will be found in
the code.

.. _doctest: http://wiki.python.org/moin/DocTest

The module `gc3libs.testing`:mod: contains a few helpers that make
writing GC3Pie tests easier.

Full documentation of the pytest_ framework is available at the `pytest`_
website.


Organizing tests
~~~~~~~~~~~~~~~~

Each single python file should have a test file inside a ``tests``
subpackage with filename created by prefixing ``test_`` to the
filename to test.  For example, if you created a file ``foo.py``,
there should be a file ``tests/test_foo.py`` which will contains tests
for ``foo.py``.

Even though following the naming convention above is not always
possible, each test regarding a specific component should be in a file
inside a ``tests`` directory inside that component.  For instance,
tests for the subpackage `gc3libs.persistence` are located inside the
directory ``gc3libs/persistence/tests`` but are not named after the
specific file.


Coding style
------------

**Python code should be written according to `PEP 8`_ recommendations.**
(And by this we mean not just the code style.)

.. _`pep 8`: http://www.python.org/dev/peps/pep-0008/

Please take the time to read `PEP 8`_ through, as it is widely-used
across the Python programming community -- it will benefit your
contribution to any free/open-source Python project!

Anyway, here's a short summary for the impatient:

* use English nouns to name variables and classes; use verbs to
  name object methods.

* use 4 spaces to indent code; never use TAB characters.

* use lowercase letters for method and variable names; use underscores
  ``_`` to separate words in multi-word identifiers (e.g.,
  ``lower_case_with_underscores``)

* use "CamelCase" for class and exception names.

* but, above all, do not blindly follow the rules and try to do the
  thing that *enhances code clarity and readability!*


Here's other code conventions that apply to GC3Pie code; since they
are not always widely followed or known, a short rationale is given
for each of them.

* Every class and function should have a docstring. Use
  reStructuredText_ markup for docstrings and documentation text
  files.

  *Rationale:* A concise English description of the purpose of a
  function can be faster to read than the code.  Also, undocumented
  functions and classes do not appear in this documentation, which
  makes them invisible to new users.

  .. _reStructuredText: http://docutils.sourceforge.net/rst.html


* Use fully-qualified names for all imported symbols; i.e., write
  ``import foo`` and then use ``foo.bar()`` instead of ``from foo
  import bar``.  If there are few imports from a module, and the
  imported names do *clearly* belong to another module, this rule can
  be relaxed if this enhances readability, but *never* do use
  unqualified names for exceptions.

  *Rationale:* There are so many functions and classes in GC3Pie, so
  it may be hard to know to which module the function `count` belongs.
  (Think especially of people who have to bugfix a module they didn't
  write in the first place.)


* When calling methods or functions that accept both positional and
  optional arguments like::

    def foo(a, b, key1=defvalue1, key2=defvalue2):

  always specify the argument name for optional arguments, which means
  **do not call**::

    foo(1, 2, value1, value2)

  but **call instead**::

    foo(1, 2, key1=value1, key2=value2)

  *Rationale:* calling the function with explicit argument names will
  reduce the risk of hit some compatibility issues. It is perfectly
  fine, from the point of view of the developer, to change the
  signature of a function by swapping two different *optional*
  arguments, so this change can happen any time, although changing
  *positional* arguments will break backward compatibility, and thus
  it's usually well advertised and tested.


* Use double quotes ``"`` to enclose strings representing messages meant
  for human consumption (e.g., log messages, or strings that will be
  printed on the users' terminal screen).

  *Rationale:* The apostrophe character ``'`` is a normal occurrence in
  English text; use of the double quotes minimizes the chances that
  you introduce a syntax error by terminating a string in its middle.


* Follow normal typographic conventions when writing user messages and
  output; prefer clarity and avoid ambiguity, even if this makes the
  messages longer.

  *Rationale:* Messages meant to be read by users *will* be read by
  users; and if they are not read by users, they will be fired back
  verbatim on the mailing list on the next request for support. So
  they'd better be clear, or you'll find yourself wondering what that
  message was intended to mean 6 months ago.

  Common typographical conventions enhance readability, and help users
  identify lines of readable text.


* Use single quotes ``'`` for strings that are meant for internal
  program usage (e.g., attribute names).

  *Rationale:* To distinguish them visually from messages to the user.


* Use triple quotes ``"""`` for docstrings, even if they fit on a single
  line.

  *Rationale:* Visual distinction.


* Each file should have this structure:

  - the first line is the `hash-bang line`__,
  - the module docstring (explain briefly the module purpose and
    features),
  - the copyright and licence notice,
  - module imports (in the order suggested by :PEP:`8`)
  - and then the code...

  *Rationale:* The docstring should be on top so it's the first thing
  one reads when inspecting a file.  The copyright notice is just a
  waste of space, but we're required by law to have it.

  .. __: http://en.wikipedia.org/wiki/Shebang_(Unix)


Documentation
-------------
The documentation can be found in gc3pie/docs. It is generated using
Sphinx (http://sphinx-doc.org/contents.html).

GC3Pie documentation is divided in three sections:

* :ref:`User Documentation`: info on how to install, configure and run
  GC3Pie applications.

* :ref:`Programmer Documentation`: info for programmers who want to
  use the GC3Pie libraries to write their own scripts and
  applications.

* :ref:`Contributor Documentation`: detailed information on how to
  contribute to GC3Pie and get your code included in the main library.

The `GC3Libs programming API <gc3libs_>` is the most relevant part of the
docs for developers contributing code and is generated automatically
from the docstrings inside the modules. Automatic documentation in
Sphinx is described under
http://sphinx-doc.org/tutorial.html#autodoc. While updating the docs
of existing modules is simply done by running ``make html``, adding
documentation for a new module requires one of the following two
procedures:

- Add a reference to the new module in
  `docs/programmers/api/index.rst`:file:. Additionally, create a file
  that enables automatic documentation for the module. For the module
  `core.py`:file:, for example, automatic documentation is enabled by
  a file `docs/programmers/api/gc3libs/core.rst`:file: with the
  following content::

    `gc3libs.core`
    ==============

    .. automodule:: gc3libs.core
         :members:

- Execute the script `docs/programmers/api/makehier.sh`:file:, which
  automates the above.  Note that the `makehier.sh`:file: script will
  re-create all ``.rst`` files for all GC3Pie modules, so check if
  there were some unexpected changes (e.g., with ``git status``)
  before you commit!

Docstrings are written in reStructuredText_ format.  To be able to
cross-reference between different objects in the documentation, you
should be familiar with `Sphinx domains`_ in general and the `Python
domain`_ in particular.

.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _`Sphinx domains`: http://sphinx-doc.org/domains.html#the-python-domain
.. _`python domain`: http://sphinx-doc.org/domains.html#cross-referencing-python-objects


Questions?
----------

Please write to the `GC3Pie mailing list`_; we try to do our best to
answer promptly.



.. (for Emacs only)
..
  Local variables:
  mode: rst
  End:
