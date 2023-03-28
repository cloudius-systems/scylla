Nodetool stop compaction
========================

Stops a compaction operation. This command is usually used to stop compaction that has a negative impact on the performance of a node.

Usage

.. code:: sh

          nodetool <options> stop <compaction_type> 

.. versionadded:: version 4.5 ``compaction type``
   
   Supported compaction types: COMPACTION, CLEANUP, VALIDATION, SCRUB, RESHARD, RESHAPE


For example:

.. code:: sh

    nodetool stop COMPACTION

    nodetool stop COMPACTION RESHAPE

.. include:: nodetool-index.rst
