The ``data/`` directory contains the reference data:
``5network.dat`` and ``45netork.dat``

.. Main developers: "Zhao Yang" <@business.uzh.ch>


Execution requirements
======================

This benchmark suite makes the assumption that all the relevant
benchmark applications (being them binaries, modules or libraries)
have been deployed and correctly installed on the destination
resource.

_Note_: On the Hobbes could infrastructure, a dedicated Appliance has
been created ``benchmark-1404``. 
Openstack image id: 53416bab-204a-4f5a-b0b4-43cffa0c5b9e

In order to run this benchmark suite, a dedicated flavor has been
prepared on the _Hobbes_ cloud infrastructure to guarantee consistency
of the benchmark results (homogeneous hardware and exclusive access).

Flavor name: benchmark

Testing ``gbenchmark``
===================

    python gbenchmark.py data/ -b infomap

The ``gbenchmark`` takes as input argument a folder containing network
data files (with .dat extension).

Optionally, it is possible to specify what benchmarks should be
executed.

For each of valid network data files found in the _input_ directory,
``gbenchmark`` will run all the selected benchmarks and collects the
corresponding results.

Invocation of ``gbenchmark`` follows the usual session-based script
conventions::

    python gbenchmark.py -s <TEST_SESSION_NAME> -C 120 -vvv
    ./data -o ./results -b <BENCHMARK>

When all the jobs are done, the _results_ directory will contain
the merged result file with the following schema:

 <benchmark_name> / <benchmark_type> / <network_filename>

How specify what benchmark to run
=================================

  python gbenchmark.py -h

provides a list of all arguments and options that could be passed. the
``-b`` option is used to select what benchmark to run.
Benchmarks are expressed in the following schema:
 <benchmark_name>-<benchmark_type>

Additionally ``gbenchmark`` allows to select group of benchmarks by
_name_ (e.g. selecting _infomap_ will run all infomap benchmarks -
python, cpp, R)

``gbenhmark`` takes a comma separated list of benchmarks (case insensitive)

Examples of valid values :
python gbenchmark.py -b infomap-cpp
python gbenchmark.py -b infomap
python gbenchmark.py -b InfoMap-R, infomap-python




