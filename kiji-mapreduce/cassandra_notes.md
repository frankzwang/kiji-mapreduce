# Notes about Cassandra / KijiMR integration

These notes are meant to go along with the notes in the Cassandra branch of Kiji Schema.  They
describe the design decisions made to provide Cassandra-backed support for Kiji MR.

# How does this work in Cassandra's Hadoop / CQL3 integration?

### Getting input splits

The C* / Hadoop code can do row scans if you use an order-preserving partitioner.  The code for
calculating the splits exists in `getInputSplits` in `AbstractColumnFamilyInputFormat`.  It looks
like the only partitioner that would support this capability is the `ByteOrderedPartioner`, which is
strongly discouraged in the DataStax
[documentation](http://www.datastax.com/documentation/cassandra/2.0/webhelp/index.html#cassandra/architecture/architecturePartitionerAbout_c.html#concept_ds_dwv_npf_fk).

TODO: Look up how we support sequential row scans in HBase / Kiji.  Do we use some kind of
order-preserving hashing?

Assuming that your partitioner is not order-preserving, the code in `getSplits` will create a new
`SplitCallable` for each individual `TokenRange`.  The token ranges come from a call in the Thrift
API to `client.describe_ring(keyspace).`  This call seemed to return to me a list as long as
whatever I had specified for `num_tokens` in my `cassandra.yaml` file.  Presumably this is something
like the total number of vnodes / token ranges in a Cassandra cluster.

The `SplitCallable` then returns a list of input splits (presumably for a given token range) by
calling `getSubSplits`.  What is happening here is really unclear and has almost no comments.  It
looks like you try to connect to all of the different RPC endpoints for the given token range
(presumably you could just contact any node in the C* cluster) and then you call either
`client.describe_splits_ex` or `client.describe_splits`.  The code for implementing this on the
server side is is `CassandraServer.java`: `describe_splits_ex`.


The bottom line is that this code seems to get the splits (as `CfSplit`, from the Thrift API) within
a server.  The implementation of this goes all of the way into `StorageService#getSplits`.  The C*
server tries to supply the correct splits with token ranges to provide splits that each contain the
specified number of keys.

TODO: Is there anything equivalent to all of this splits business for the DataStax API?

Finally back within the `SplitCallable` method `call`, we go from a list of `CfSplit` objects to a
list of `ColumnFamilySplit` objects (we get out of the Thrift API an into the Java C* / Hadoop
code!).

So eventually these are all of the input splits, each of which contains a start token, and end
token, and a list of the data nodes hosting the data (so that Hadoop can assign the map tasks to
nodes that are close to these storage nodes).

### The RecordReader

Each `CqlPagingRecordReader` is given an input split, which we saw above is just a start token, an
end token, and a list of replica nodes containing rows in the token range.

It seems to me like we could replace most of this code by using the DataStax Java driver to do a
simple query with paging and just return row after row after row.  The code for `whereClause`, which
limits a query to just the data on a particular node, we could keep.

`CqlPagingRecordReader` queries the `system.schema_columnfamilies` table to get information about
key aliases, column aliases, key validator, and comparator.
- It uses the `key_aliases` to populate `partitionBoundColumns`
- It uses the `column_aliases` to populate `clusterColumns`
- It uses the `key_validator` to add key validators to the columns in `partitionBoundColumns`.  It
  looks like there is one key validator for each element of `partitionBoundColumns`.
- This looks kind of like it is getting the columns that make up the partition key
  (`partitionBoundColumns`) and the other, non-partition-key columns (`clusterColumns`).

Ah, the DataStax Java driver has this equivalent information within the
[TableMetadata](http://www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/TableMetadata.html)
class.  The "validator" business I think also just refers to the column type.

`CqlPagingRecordReader` then creates an instance of `RowIterator`, which looks similar to the Kiji
row scanner and presumably implements most of the functionality of the `RecordReader`.
`CqlPagingRecordReader` has a method `preparedQueryBindValues`, which _looks_ like it specifies
starting and ending partition keys, based on the input split.


# Code to change:

We will most likely need to create a new version of `KijiTableInputFormat` and the `RecordReader`
within that class.

We also need to find all of the places in the code where we reference `KijiTableInputFormat` and
update them such that we can sometimes produce a `CassandraKijiTableInputFormat` if necessary.
Hopefully those places in the code will also have KijiURIs present!

Usages:
- `KijiTableMapReduceJobInput`.  Within the `configure` code, it calls
  `KijiTableInputFormat.configureJob`.  There is a URI present here.
- Within `KijiTableInputFormat` itself, it calls
  `job.setInputFormatClass(KijiTableInputFormat.class);` within the code called above from
  `KijiTableMapReduceJobInput`.
- `KijiTableMapReduceJobInput`.  It returns `KijiTableInputFormat.class` from the `getInputFormat`
  method.

So the question is whether we want `KijiTableMapReduceJobInput` to choose the `InputFormat` based on
its `mInputTableURI` member, or whether we want to have two versions of
`KijiTableMapReduceJobInput`.  Or something else.

For now, we will have `KijiTableMapReduceJobInput` pick the appropriate `InputFormat` (HBase or
Cassandra) based on the URI.













# Stuff that we have to change

#### KijiTableInputFormat

This has a lot of calls to HBase-specific code.  Probably we'll have to refactor most of this code
into a separate HBase class.

`KijiTableInputFormat` also contains the class `KijiTableRecordReader`, which is where most of the
Cassandra-specific code has to go.
- We can probably just modify the record reader in the Cassandra CQL code.
- The HBase-backed one just creates a `KijiRowScanner` and uses that to iterate over the range of
  entity IDs for a given split.  It is not clear whether we can do the same thing for Cassandra.

**TODO: Check how HBase divides up keys over different machines.  Do we always expect to have
contiguous sections of keys on the same machine?**

# Random notes for me

- Why do we have `MapReduceJobInput` and `MapReduceJobOutput` interfaces?  Aren't these somewhat
  redundant with the Hadoop `InputFormat` and `OutputFormat` classes?  Are these just here to
  provide a somewhat nicer interface to what Hadoop provides?  Note that classes implementing these
  interfaces look they like will contain `InputFormat` or `OutputFormat` references, but they do not
  implement the interfaces for `InputFormat` or `OutputFormat`.  Presumably, therefore, the various
  mappers and reducers that we provide don't implement those interfaces, either (at least at the
  level at which the user interacts with them).
- `KijiContext` and `KijiTableContext` look like they provide some sensible methods for contexts for
  Kiji-based MR jobs.  I expect to see later some concrete classes that implement these interfaces
  and also extend / wrap the regular Hadoop `Configuration` class.
- `KVOutputJob` again looks like it might be designed to wrap around a bunch of
  independent-but-related functionality in Hadoop (i.e., specifying output key and value classes for
  a job configuration).
- `KijiMapReduceJob` is a wrapper around the standard Hadoop `Job.`  It can write out job history
  information to a special history table for the given Kiji instance.
- `KijiMapReduceJobBuilder` is a much more sane way (relative to straight Hadoop) of configuring a
  MapReduce job.
  - There is also `o.k.m.framework.MapReduceJobBuilder`, which contains a standard Hadoop
    `Configuration`, key-value stores, and a `MapReduceJobOutput` instance.  It also has some handy
    methods for putting JAR files into the distributed cache.
- `KijiMapper` and `KijiReducer` are pretty straightforward extensions of the base Hadoop `Mapper`
  and `Reducer` classes.


# Trace through the code when we start a job from the command line to see what happens

At the top of the command-line tool hierarchy is `BaseTool` from KijiSchema.  Within KijiMR, we
start with `JobTool`, which extends `BaseTool` and uses a `MapReduceJobBuilder`.
- `MapReduceJobBuilder` provides some convenience methods for specifying mappers, combiners, and
  reducers.
- `MapReduceJobBuilder` has a method `protected void configureJob(Job job)`, which seems to handle
  most of the job configuration.  It has abstract methods for getting the `MapReduceJobInput`,
  mapper, combiner, reducer, and JAR class.
- `MapReduceJobBuilder` has subclasses for bulk importing, for Kiji MapReduce jobs, and for jobs
  that read from Kiji tables.
- Likewise, `JobTool` has subclasses for bulk importing, kiji jobs, and map reduce jobs.
- Subclasses of `JobTool` will create instances of a subclass of `MapReduceJobBuilder`.

`o.k.m.tools.framework.KijiJobTool` is a (concrete) subclass of `JobTool` for running MapReduce jobs
over Kiji tables.  The class has template parameter that is a subclass of
`KijiTableInputJobBuilder` (instead of general `MapReduceJobBuilder`).
- The sublcasses of `KijiJobTool` include `KijiGather`, `KijiPivot`, and `KijiProduce`, so this is
  finally the end of the line of abstract classes!
- The `KijiJobTool` adds command-line flags for specifying starting and ending entity IDs.
- `KijiTableInputJobBuilder` likewise includes member variables that specifying a table KijiURI,
  starting and ending entity IDs and row filters.
  - It also defines an abstract method to get a `KijiDataRequest` that subclasses must implement.
  - It includes a method as well that validates the `KijiDataRequest` against the layout for the
    specified Kiji table.
  - It provides a concrete implementation of `getJobInput`, which returns an instance of
    `KijiTableMapReduceJobInput`: `return
    MapReduceJobInputs.newKijiTableMapReduceJobInput(mInputTableURI, getDataRequest(), rowOptions);`

We finally get down to a concrete class with `o.k.m.tools.KijiProduce`, which uses the
`KijiProduceJobBuilder.`
- `KijiProduce` includes an additional member variable for the `KijiTableMapReduceJobOutput` for the
  Producer (Producers always write to tables).
- The `KijiTableMapReduceJobOutput` right now can be either `DirectKijiTableMapReduceJobOutput` or
  `HFileMapReduceJobOutput`.  **We may need to add classes for direct Kiji/Cassandra output and for
  Cassandra BulkOutputFormat.**
- The `KijiProduceJobBuilder` includes member instances for the Producer, the mapper, the reducer
  (set internally to be an `IdentityReducer`), the `KijiTableMapReduceJobOutput`, and the
  data request.


# Job configuration elements that I had to set to get a basic Cassandra / Hadoop job to work

For reading from a table:

- Input format to `CqlPagingInputFormat`
- Input RPC port, address
- Input column family (keyspace and table)
- Input partitioner (presumably whatever you are using for the cluster in question, weird that I had
  to specify this...)
- Input CQL page row size (seems like something that the user should be able to configure in the
  data request, should be possible to hack the C* / Hadoop code to allow putting this into the
  query).
- Input split size

For writing to a table:

- Output format to `CqlOutputFormat`
- Specify the put query
- Output address, partitioner
- Output key and value class (to `Map` and `List`, respectively)

### Translating this stuff to work with Kiji

All of the information in Kiji for setting up the configuration for a `KijiMapper` lives in the actual
`KijiMapper` class.
- `setConf`
- `getOutputKeyClass`
- `getOutputValueClass`

The concrete `Producer`, `Gatherer`, etc. mappers and reducers may be where we have to start making
changes.  **Right now, the producer assumes that it will output `o.k.m.framework.HFileKeyValue`
keys.**  We might be able to use this class for the Cassandra implementation, unclear.

A `KijiTableMapper` right now extends `KijiMapper<EntityId, KijiRowData, K, V>`, which then extends
the regular Hadoop `Mapper<EntityId, KijiRowData, K, V>.`

**What do we have to modify to get a Cassandra Mapper to get EntityId and KijiRowData instead of its
current mess?** ("Current mess" = input keys and values = `Map<String, ByteBuffer>, Map<String,
ByteBuffer>`)  Presumably in the `InputFormat`.

**Yes, in KijiTableInputFormat**, specifically most likely in the `RecordReader`.


# Where does HBase stuff get imported?

Very few places!  Let's skip everything for HFiles (since those are somewhat orthogonal to
integrating KijiMR and C* KijiSchema).  Also this list ignores files that import only `HConstants`
or `util.Bytes.`  (And this excludes anything in `test`)

- `o.k.m.framework.KijiTableInputFormat`
- `o.k.m.framework.MapReduceJobBuilder`
- `o.k.m.impl.KijiTableMapper`
  - This usage is no big deal, just an `instanceof TableSplit` call for some logging.
- `o.k.m.impl.KijiTableSplit`
- `o.k.m.kvstore.lib.KijiTableKeyValueStore`
- `o.k.m.output.KijiTableMapReduceJobOutput`




# Open issues

### HFiles

Lots of the KijiMR code involves writing job outputs to HFiles and then reading those HFiles into
tables.  How can we integrate this functionality efficiently with Cassandra?

- Is there a C* equivalent of an HFile?  Most C* documentation indicates that we should be able to
  load a table often by just writing to it directly.
- DataStax has a blog post about an `sstableloader` tool.
- It looks like they added some [support](https://issues.apache.org/jira/browse/CASSANDRA-3045) for
  having a C* Hadoop job output files for bulk loading.
- The DataStax documentation for Cassandra 1.1
  [indicates](https://issues.apache.org/jira/browse/CASSANDRA-3045)
  that we can use something similar to HFiles called `BulkOutputFormat`.
- This [talk](http://www.slideshare.net/knewton/hadoop-and-cassandra-24950937) also looks useful.

# TODO:

- Refactor code to put HBase-specific stuff into subpackages of `o.k.m.*.impl`


# Places that look like they may be important

- `o.k.m.framework` contains `KijiTableInputFormat` and the associated `RecordReader`
