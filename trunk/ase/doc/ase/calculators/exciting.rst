.. module:: exciting

========
exciting
========

.. image:: ../../_static/exciting.png

Introduction
============

``exciting`` is a full-potential *all-electron*
density-functional-theory (:term:`DFT`) package based on the
linearized augmented planewave (:term:`LAPW`) method. It can be
applied to all kinds of materials, irrespective of the atomic species
involved, and also allows for the investigation of the core
region. The website is http://exciting-code.org/

The module depends on lxml  http://codespeak.net/lxml/


Exciting Calculator Class
=========================

.. autoclass:: ase.calculators.exciting.Exciting

There are two ways to construct the exciting calculator.

Constructor Keywords
--------------------

One is by giving parameters of the ground state in the
constructor. The possible attributes can be found at
http://exciting-code.org/input-reference#groundstate.

.. class:: Exciting(bin='excitingser', kpts=(4, 4, 4), xctype='GGArevPBE')

XSLT Template
-------------
The other way is to use an XSLT template

.. class:: Exciting( template='template.xsl'  , bin='excitingser')

The template may look like:

.. highlight:: xml

::

    <?xml version="1.0" encoding="UTF-8" ?>
    <xsl:stylesheet version="1.0"
      xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
      <xsl:output method="xml" />
      <xsl:template match="/">
        <xsl:comment>
          created from template
        </xsl:comment>
    
    
    <!-- ############# -->
        <input>
          <title></title>
          <structure speciespath="./">
            <xsl:copy-of select="/input/structure/crystal" />
            <xsl:copy-of select="/input/structure/species" />
          </structure>
          <groundstate ngridk="4  4  4"  vkloff="0.5  0.5  0.5" tforce="true" />
        </input>
    <!-- ############# -->
    
    
    
      </xsl:template>
    </xsl:stylesheet>
    
.. highlight:: python    

Current Limitations
===================

The calculator supports only total energy and forces no stress strain
implemented in exciting yet.  However it is planned to be implemented
soon. http://exciting-code.org/current-developments.

The keywords on the constructor are converted into attributes 
of the ``groundstate`` element. No check is done there and not all 
options are accessible in that way. By using the template one can 
exceed this limitation.


