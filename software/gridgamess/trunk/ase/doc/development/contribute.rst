=================
How to contribute
=================

Discussion of ASE development takes place on the :ref:`ase-developer
<mailing_lists>` mailing list, and also sometimes on the #gpaw IRC
channel on freenode.

We welcome new developers who would like to help work on improving
ASE.  If you would like to contribute, your should first tell us what
you want to work on.  Use the mailing list for that.


SVN access
==========

We don't give new contributers write access to our SVN repository from
day one.  So, you will have to create a patch and send it to the
mailing list::

  $ svn checkout https://svn.fysik.dtu.dk/projects/ase/trunk myase
  $ cd myase
  $ # do your thing ...
  $ svn diff > patch.txt

One of the current committers will look at the patch and give you
some feedback.  Maybe the patch is fine and the committer will commit
it to trunk.  There could also be some more work to do like:

* write more comments
* follow our :ref:`python_codingstandard`
* fix this docstring
* write a test
* add some documentation

Once everyone is happy, the patch can be appied.  This patch-feedback
loop is not something we have invented to prevent you from
contributing - it should be viewed as an opportunity for you to learn
how to write good code that fits into the ASE codebase.  

After a couple of contributions, we will probably trust you enough to
add you as a committer.


