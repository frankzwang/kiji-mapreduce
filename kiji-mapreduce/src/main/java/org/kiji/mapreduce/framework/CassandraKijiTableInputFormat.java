/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.framework;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.hadoop2.multiquery.ConfigHelper;
import org.apache.cassandra.hadoop2.multiquery.CqlQuerySpec;
import org.apache.cassandra.hadoop2.multiquery.MultiQueryCqlInputFormat;
import org.apache.cassandra.hadoop2.multiquery.MultiQueryRecordReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.CassandraKijiURI;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReaderBuilder;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.impl.cassandra.CQLUtils;
import org.kiji.schema.impl.cassandra.CassandraByteUtil;
import org.kiji.schema.impl.cassandra.CassandraKiji;
import org.kiji.schema.impl.cassandra.CassandraKijiRowData;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.impl.cassandra.CassandraKijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.ResourceUtils;

/**
 * InputFormat for Hadoop MapReduce jobs reading from a Cassandra-backed Kiji table.
 *
 * Wraps around the Cassandra CQL3 Hadoop InputFormat class to convert from raw Cassandra data into
 * Kiji data.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CassandraKijiTableInputFormat
    extends InputFormat<EntityId, KijiRowData>
    implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableInputFormat.class);

  /** Configuration of this input format. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<EntityId, KijiRowData> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException {
    return new CassandraKijiTableRecordReader(mConf);
  }

  /** Store instance of Cassandra's InputFormat so that we can wrap around its various methods. */
  private MultiQueryCqlInputFormat mInputFormat;

  /**
   * Constructor for the input format.
   */
  public CassandraKijiTableInputFormat() {
    super();
    mInputFormat = new MultiQueryCqlInputFormat();
  }

  /**
   * Configure all of the Cassandra-specific stuff right before calling the Cassandra code for
   * getInputSplits.
   *
   * @param conf Hadoop Configuration for the MR job.
   * @throws IOException if there is a problem reading table layout information.
   */
  private void setCassandraSpecificConfiguration(Configuration conf) throws IOException {

    final KijiURI inputTableURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
    assert(inputTableURI.isCassandra());

    final Kiji kiji = Kiji.Factory.open(inputTableURI);
    assert(kiji instanceof CassandraKiji);
    final CassandraKiji cassandraKiji = (CassandraKiji) kiji;
    final KijiTable table = cassandraKiji.openTable(inputTableURI.getTable());
    assert(table instanceof CassandraKijiTable);
    final CassandraKijiTable cassandraTable = (CassandraKijiTable) table;

    // Get data request from the job configuration.
    final String dataRequestB64 = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
    Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
    final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));

    KijiDataRequest dataRequest =
        (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    Preconditions.checkNotNull(dataRequest);

    LOG.info("Translating Kiji data request into Cassandra queries.");

    DataRequestToQuerySpecsConverter converter = new DataRequestToQuerySpecsConverter(
        dataRequest,
        cassandraTable
    );

    for (CqlQuerySpec cqlQuerySpec : converter.makeQueries()) {
      LOG.info("Adding CQL query " + cqlQuerySpec);
      ConfigHelper.setInputCqlQuery(conf, cqlQuerySpec);
    }

    // Get a list of all of the columns used for the entity ID.
    KijiTableLayout layout = cassandraTable.getLayout();

    // Get the clustering columns that are part of the EntityID.
    List<String> clusteringColumns = CQLUtils.getClusterColumns(layout);
    Preconditions.checkArgument(clusteringColumns.containsAll(Lists.newArrayList(
        CQLUtils.LOCALITY_GROUP_COL,
        CQLUtils.FAMILY_COL,
        CQLUtils.QUALIFIER_COL,
        CQLUtils.VERSION_COL
    )));
    clusteringColumns.removeAll(Lists.newArrayList(
        CQLUtils.LOCALITY_GROUP_COL,
        CQLUtils.FAMILY_COL,
        CQLUtils.QUALIFIER_COL,
        CQLUtils.VERSION_COL
    ));

    List<String> partitionKeyColumns = CQLUtils.getPartitionKeyColumns(layout);
    LOG.info("Clustering columns (in entity ID) = " + clusteringColumns);
    LOG.info("Partitioning columns = " + partitionKeyColumns);
    ConfigHelper.setInputCqlQueryClusteringColumns(
        conf, clusteringColumns.toArray(new String[clusteringColumns.size()]));

    // TODO: Allow the user to use a different partitioner.
    CassandraKijiURI cassandraInputTableURI = (CassandraKijiURI) inputTableURI;
    final List<String> cassandraHosts = cassandraInputTableURI.getCassandraNodes();
    final int cassandraPort = cassandraInputTableURI.getCassandraClientPort();

    ConfigHelper.setInputNativeTransportContactPoints(
        conf, cassandraHosts.toArray(new String[cassandraHosts.size()]));
    ConfigHelper.setInputNativeTransportPort(conf, cassandraPort);

    // TODO: Allow user to specify target number of splits.
    cassandraTable.release();
    kiji.release();
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final KijiURI inputTableURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
    assert(inputTableURI.isCassandra());

    LOG.info("Getting input splits.");

    //final Kiji kiji = Kiji.Factory.open(inputTableURI, conf);

    // Set all of the Cassandra-specific configuration stuff right before calling the Cassandra
    // code!
    setCassandraSpecificConfiguration(conf);

    return mInputFormat.getSplits(context);
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process. May be left null to indicate
   *     that scanning should start at the beginning of the table.
   * @param endRow Maximum row key to process. May be left null to indicate that
   *     scanning should continue to the end of the table.
   * @param filter Filter to use for scanning. May be left null.
   * @throws java.io.IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      KijiURI tableURI,
      KijiDataRequest dataRequest,
      EntityId startRow,
      EntityId endRow,
      KijiRowFilter filter
  ) throws IOException {
    Preconditions.checkNotNull(job, "job must not be null");
    Preconditions.checkNotNull(tableURI, "tableURI must not be null");
    Preconditions.checkNotNull(dataRequest, "dataRequest must not be null");
    Preconditions.checkArgument(tableURI.isCassandra());
    Preconditions.checkArgument(tableURI.toString().startsWith("kiji-cassandra"));

    final Configuration conf = job.getConfiguration();

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // Write all the required values to the job's configuration object.
    job.setInputFormatClass(CassandraKijiTableInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
    LOG.info("Writing Kiji table URI \"" + tableURI + "\" to Configuration.");
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableURI.toString());

    // TODO: Need to pick a better exception class here...
    if (null != startRow) {
      throw new KijiIOException("Cannot specify a start row for C* KijiMR jobs");
    }
    if (null != endRow) {
      throw new KijiIOException("Cannot specify an end row for C* KijiMR jobs");
    }
    if (null != filter) {
      conf.set(KijiConfKeys.KIJI_ROW_FILTER, filter.toJson().toString());
    }
  }

  /** Hadoop record reader for Kiji table rows. */
  public static final class CassandraKijiTableRecordReader
      extends RecordReader<EntityId, KijiRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableRecordReader.class);

    /** Data request. */
    private final KijiDataRequest mDataRequest;
    private final MultiQueryRecordReader mRecordReader;

    private Kiji mKiji = null;
    private CassandraKijiTable mTable = null;
    private EntityIdFactory mEntityIdFactory;
    private KijiRowData mCurrentRow = null;

    private KijiTableLayout mLayout;

    // We need a reader to transform Cassandra Rows into CassandraKijiDataRows.
    private CassandraKijiTableReader mReader;

    private final CellDecoderProvider mCellDecoderProvider;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Kiji.
     *
     * @param conf Configuration for the target Kiji.
     * @throws java.io.IOException if there is a problem reading table layout information.
     */
    private CassandraKijiTableRecordReader(Configuration conf) throws IOException {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
      mRecordReader = new MultiQueryRecordReader();

      String uriString = conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI);
      LOG.info("Read URI from conf: " + uriString);

      final KijiURI inputURI =
          CassandraKijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
      mKiji = Kiji.Factory.open(inputURI);
      mTable = (CassandraKijiTable)mKiji.openTable(inputURI.getTable());
      mLayout = mTable.getLayout();
      mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());

      // Get a bunch of stuff that we'll need to go from a Row to a CassandraKijiRowData.
      mReader = CassandraKijiTableReader.create((CassandraKijiTable)mTable);

      mCellDecoderProvider = new CellDecoderProvider(
          mTable.getLayout(),
          Maps.<KijiColumnName, BoundColumnReaderSpec>newHashMap(),
          Sets.<BoundColumnReaderSpec>newHashSet(),
          KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      LOG.info("Creating Cassandra table record reader...");
      mRecordReader.initialize(split, context);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getCurrentKey() throws IOException {
      return mCurrentRow.getEntityId();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData getCurrentValue() throws IOException {
      return mCurrentRow;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      if (mCurrentRow == null) {
        return 0.0f;
      }
      return 1.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      boolean hasNext = mRecordReader.nextKeyValue();
      if (!hasNext) {
        mCurrentRow = null;
        return false;
      }

      List<Row> rows = mRecordReader.getCurrentValue();
      assert(null != rows);

      // Figure out the entity ID from the row.
      KijiRowKeyComponents entityIDComponents = CQLUtils.getRowKeyComponents(mLayout, rows.get(0));
      EntityId entityID = mEntityIdFactory.getEntityId(entityIDComponents);
      // Now create a KijiRowData with all of these rows.
      try {
        mCurrentRow =
            new CassandraKijiRowData(mTable, mDataRequest, entityID, rows, mCellDecoderProvider);
      } catch (IOException ioe) {
        throw new KijiIOException("Cannot create KijiRowData.", ioe);
      }

      return true;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.closeOrLog(mReader);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mKiji);

      mReader = null;
      mTable = null;
      mKiji = null;
    }
  }

  /**
   * This class is responsible for taking a KijiDataRequest and turning it into a list of
   * CqlQuerySpecs for the Cassandra InputFormat.
   *
   * This code is very similar to what exists in the CassandraDataRequestAdapter in KijiSchema.
   */
  private static final class DataRequestToQuerySpecsConverter {
    private static final Logger LOG =
        LoggerFactory.getLogger(DataRequestToQuerySpecsConverter.class);
    private final KijiDataRequest mKijiDataRequest;
    private final CassandraColumnNameTranslator mColumnNameTranslator;
    private final CassandraKijiTable mTable;

    /**
     * Constructor for the KijiDataRequest-to-CqlQuerySpec converter.
     *
     * @param kijiDataRequest The data request to translate.
     * @param table The table the data request queries.
     */
    public DataRequestToQuerySpecsConverter(
        KijiDataRequest kijiDataRequest,
        CassandraKijiTable table) {
      mKijiDataRequest = kijiDataRequest;
      mTable = table;
      mColumnNameTranslator = CassandraColumnNameTranslator.from(mTable.getLayout());
    }

    /**
     * Create a set of CqlQuerySpecs for the C* Hadoop InputFormat, given a data request.
     *
     * @return The set of query specs.
     * @throws IOException If there is a problem getting table layout information.
     */
    private Set<CqlQuerySpec> makeQueries() throws IOException {
      // Get the Cassandra table name for non-counter values.
      final CassandraTableName nonCounterTableName =
          CassandraTableName.getKijiTableName(mTable.getURI());

      // Get the counter table name.
      final CassandraTableName counterTableName =
          CassandraTableName.getKijiCounterTableName(mTable.getURI());

      // TODO: Min/max versions

      // A single Kiji data request can result in many Cassandra queries, so we use asynchronous IO
      // and keep a list of all of the futures that will contain results from Cassandra.
      Set<CqlQuerySpec> querySpecs = Sets.newHashSet();

      // For now, to keep things simple, we have a separate request for each column, even if there
      // are multiple columns of interest in the same column family that we could potentially put
      // together into a single query.
      for (KijiDataRequest.Column column : mKijiDataRequest.getColumns()) {
        LOG.info("Processing data request for data request column " + column);
        if (column.isPagingEnabled()) {
          // The user will have to use an explicit KijiPager to get this data.
          LOG.info("...this column is paged, but this is not a KijiPager request, skipping...");
          continue;
        }

        // Translate the Kiji column name.
        KijiColumnName kijiColumn = new KijiColumnName(column.getName());
        LOG.info("Kiji column name for the requested column is " + kijiColumn);
        CassandraColumnName cassandraColumn =
            mColumnNameTranslator.toCassandraColumnName(kijiColumn);

        Preconditions.checkArgument(cassandraColumn.containsFamily(),
            "Column must contain a locality group and family.");

        // Determine whether we need to read non-counter values and/or counter values.
        List<CassandraTableName> tableNames = Lists.newArrayList();
        if (maybeContainsNonCounterValues(mTable, kijiColumn)) {
          tableNames.add(nonCounterTableName);
        }
        if (maybeContainsCounterValues(mTable, kijiColumn)) {
          tableNames.add(counterTableName);
        }

        for (CassandraTableName cassandraTableName : tableNames) {
          // Just fetch all columns.

          // WHERE clauses:
          StringBuilder sb = new StringBuilder();
          List<Serializable> whereArgs = Lists.newArrayList();
          sb
              .append(" WHERE ")
              .append(CQLUtils.LOCALITY_GROUP_COL)
              .append("=? AND ")
              .append(CQLUtils.FAMILY_COL)
              .append("=?");
          whereArgs.add(cassandraColumn.getLocalityGroup());
          whereArgs.add(CassandraByteUtil.byteBuffertoBytes(cassandraColumn.getFamilyBuffer()));

          if (cassandraColumn.containsQualifier()) {
            sb.append(" AND ")
                .append(CQLUtils.QUALIFIER_COL)
                .append("=?");
            whereArgs.add(CassandraByteUtil.byteBuffertoBytes(
                cassandraColumn.getQualifierBuffer()));
          }
          String whereClause = sb.toString();

          // Break up the table name into separate keyspace and table (without quotes).
          CqlQuerySpec query = CqlQuerySpec.builder()
              .withKeyspace(cassandraTableName.getKeyspace())
              .withTable(cassandraTableName.getTable())
              .withWhereClause(whereClause, whereArgs.toArray(new Serializable[whereArgs.size()]))
              .build();
          querySpecs.add(query);
        }
      }

      if (querySpecs.isEmpty()) {
        // Add a dummy entity ID scan...

        // Break up the table name into separate keyspace and table (without quotes).
        CqlQuerySpec query = CqlQuerySpec.builder()
            .withKeyspace(nonCounterTableName.getKeyspace())
            .withTable(nonCounterTableName.getTable())
            .build();
        querySpecs.add(query);
      }

      return querySpecs;
    }

    /**
     *  Check whether this column could specify non-counter values.  Return false iff this column
     *  name refers to a fully-qualified column of type COUNTER or a map-type family of type
     *  COUNTER.
     *
     * @param table The table to check for counters.
     * @param kijiColumnName The column to check for counters.
     * @return whether this column could contain non-column values.
     * @throws IOException if there is a problem reading the table.
     */
    private boolean maybeContainsNonCounterValues(
        CassandraKijiTable table,
        KijiColumnName kijiColumnName
    ) throws IOException {
      boolean isNonCounter = true;
      try {
        // Pick a table name depending on whether this column is a counter or not.
        if (table
            .getLayout()
            .getCellSpec(kijiColumnName)
            .isCounter()) {
          isNonCounter = false;
        }
      } catch (IllegalArgumentException e) {
        // There *could* be non-counter values here.
      }
      return isNonCounter;
    }

    /**
     *  Check whether this column could specify counter values.  Return false iff this column name
     *  refers to a fully-qualified column that is not of type COUNTER or a map-type family not of
     *  type COUNTER.
     *
     * @param table to check for counter values.
     * @param kijiColumnName to check for counter values.
     * @return whether the column *may* contain counter values.
     * @throws IOException if there is a problem reading the table.
     */
    private boolean maybeContainsCounterValues(
        CassandraKijiTable table,
        KijiColumnName kijiColumnName
    ) throws IOException {
      boolean isCounter = false;
      try {
        // Pick a table name depending on whether this column is a counter or not.
        isCounter = table
            .getLayout()
            .getCellSpec(kijiColumnName)
            .isCounter();
      } catch (IllegalArgumentException e) {
        // There *could* be counters here.
        isCounter = true;
      }
      return isCounter;
    }
  }
}
