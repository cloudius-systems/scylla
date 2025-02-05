# Keeping sstables on S3

On of the ways to use object storage is to keep sstables directly on it as objects.

## Enabling the feature

Currently the object-storage backend works if `keyspace-storage-options` is listed
in `experimental_features` in `scylla.yaml`. like:

```yaml
experimental_features:
  - keyspace-storage-options
```

It can also be enabled with `--experimental-features=keyspace-storage-options`
command line option when launchgin scylla.

## Creating keyspace

Sstables location is keyspace-scoped. In order to create a keyspace with S3
storage use `CREATE KEYSPACE` with `STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' }`
parameters, where `$endpoint_name` should match with the corresponding `name`
of the configured endpoint in the YAML file above.

In the following example, an endpoint named "s3.us-east-2.amazonaws.com" is
defined in `object_storage.yaml`, and this endpoint is used when creating the
keyspace "ks".

in `object_storage.yaml`:

```yaml
endpoints:
  - name: s3.us-east-2.amazonaws.com
    port: 443
    https: true
    aws_region: us-east-2
    aws_access_key_id: EXAMPLE_ACCESS_KEY_ID
    aws_secret_access_key: EXAMPLE_SECRET_ACCESS_KEY
    is_default: true
```

Please note that you need to specify at least one endpoint in `object_storage.yaml`.
If you specify more than one endpoint, one of them needs to be marked `is_default: true`,
if you specify only one, the default flag is optional, Scylla will assume that's the default
anyway.

and when creating the keyspace:

```cql
CREATE KEYSPACE ks
  WITH REPLICATION = {
   'class' : 'NetworkTopologyStrategy',
   'replication_factor' : 1
  }
  AND STORAGE = {
   'type' : 'S3',
   'endpoint' : 's3.us-east-2.amazonaws.com',
   'bucket' : 'bucket-for-testing'
  };
```

# Copying sstables on S3 (backup)

It's possible to upload sstables from data/ directory on S3 via API. This is good
to do because in that case all the resources that are needed for that operation (like
disk IO bandwidth and IOPS, CPU time, networking bandwidth) will be under Seastar's
control and regular Scylla workload will not be randomly affected.

The API endpoint name is `/storage_service/backup` and its Swagger description can be
found [here](./api/api-doc/storage_service.json). Accepted parameters are

* *keyspace*: the keyspace to copy sstables from
* *table*: the table to copy sstables from
* *snapshot*: the snapshot name to copy sstables from
* *endpoint*: the key in the object storage configuration file
* *bucket*: bucket name to put sstables' files in
* *prefix*: prefix to put sstables' files under

Currently only snapshot backup is possible, so first one needs to take [snapshot](docs/kb/snapshots.rst)

All tables in a keyspace are uploaded, the destination object names will look like
`s3://bucket/some/prefix/to/store/data/.../sstable`
