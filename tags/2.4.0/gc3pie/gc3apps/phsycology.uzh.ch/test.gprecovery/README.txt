The ``gprecovery`` application allows to run ``RecoveryFunction``
Matlab function over a different set of recovery models.

Testing ``gprecovery``
======================

Invocation of ``gprecovery`` follows the usual session-based script
conventions::
    gprecovery -s SESSION_NAME -C 120 [models]

* [models] is a list of models to run the RecoveryFunction on. The
  syntax supported is the following:
    1:N | N1,N2,..,Nm | N

  Examples: 1:4 , 1,3,4 | 2

  Current valid model range: 1 - 4


``gprecovery`` has the following extra options:

	      
    -b [STRING], --binary [STRING]
                        Location of the Matlab compiled binary version of the
                        ParRecoveryFun. Default: None.
    -E [int], --random_range [int]
                        Upper limit for the random seed used in the fmin
                        function. Default: 1000.
    -R [int], --repeat [int]
                        Repeat all simulation [repeat] times. Default: 1 (no
                        repeat).

Using gprecovery
================

To launch al the simulations, just specify the number of models and
the number of repetitions and the binary to be executed:

    gprecovery 1:4 -R 500 -b <location_of_compiled_ParRecovery> -C 60
    -o results




