================
Nodetool restore
================

**restore** - Load SSTables from a designated bucket in object store into a specified keyspace or table

The status of a restore operation is retained for a period defined by ``user_task_ttl``,
allowing you to query the status even after the operation completes.
You can configure the TTL duration using the :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>` command.

To run the command asynchronously, use the ``--nowait`` flag. The command will then print a task ID.
You can use this ID with :doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>` command to track the progress of the restore operation or
to cancel it.

To run the command synchronously, run it without the ``--nowait`` flag. In this case,
the command relies on ``user_task_ttl`` internally.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
               --endpoint <endpoint> --bucket <bucket>
               --prefix <prefix>
               --keyspace <keyspace>
               --table <table>
               [--nowait]
               <sstables>...

Example
-------

.. code-block:: console

   nodetool restore --endpoint s3.us-east-2.amazonaws.com  --bucket bucket-foo --prefix ks/cf/24601 --keyspace ks --table cf \
     scylla/ks/cf/34/me-3gdq_0bki_2dy4w2gqj6hoso4mw1-big-TOC.txt \
     scylla/ks/cf/34/me-3gdq_0bki_2dipc1ysb2x2a3btgh-big-TOC.txt \
     scylla/ks/cf/42/me-3gdq_0bki_2s3e829t3gyq994yjl-big-TOC.txt


Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``--endpoint`` - Name of the configured object storage endpoint to load SSTables from.
  This should be configured as per :ref:`the object storage configuration instructions <object-storage-configuration>`.
* ``--bucket`` - Name of the bucket to load SSTables from
* ``--prefix`` - The share prefix for object keys of backed up SSTables
* ``--keyspace`` - Name of the keyspace to load SSTables into
* ``--table`` - Name of the table to load SSTables into
* ``--nowait`` - Returns immediately without waiting for the restore operation to complete.
* ``<sstables>`` - Remainder of keys of the TOC (Table of Contents) components of SSTables to restore, relative to the specified prefix

See also

:doc:`Nodetool backup </operating-scylla/nodetool-commands/backup/>`

.. include:: nodetool-index.rst
