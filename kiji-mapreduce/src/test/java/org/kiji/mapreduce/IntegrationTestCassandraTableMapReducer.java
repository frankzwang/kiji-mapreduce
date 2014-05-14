/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraKijiInstaller;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.InstanceBuilder;

/** Tests running a table map/reducer. */
public class IntegrationTestCassandraTableMapReducer extends AbstractKijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestCassandraTableMapReducer.class);

  private static Kiji mKiji;
  private static KijiURI mUri;
  private static KijiURI mTableUri;

  @BeforeClass
  public static void populateTable() throws Exception {
    final Configuration conf = HBaseConfiguration.create();

    // Create a Kiji instance.
    mUri = KijiURI.newBuilder(String.format(
        "kiji-cassandra://%s:%s/127.0.0.10/9042/test",
        conf.get(HConstants.ZOOKEEPER_QUORUM),
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT)
    )).build();
    LOG.info("Installing to URI " + mUri);
    try {
      CassandraKijiInstaller.get().install(mUri, conf);
      LOG.info("Created Kiji instance at " + mUri);
    } catch (IOException ioe) {
      LOG.warn("Could not create Kiji instance.");
      assertTrue("Did not start.", false);
    }

    // Create a table with a simple table layout.
    final Kiji kiji = Kiji.Factory.open(mUri, conf);
    try {
      final KijiTableLayout layout = KijiTableLayouts.getTableLayout("org/kiji/mapreduce/layout/foo-test-rkf2.json");
      final long timestamp = System.currentTimeMillis();

      // Insert some data into the table.
      new InstanceBuilder(kiji)
          .withTable(layout.getName(), layout)
          .withRow("gwu@usermail.example.com")
          .withFamily("info")
          .withQualifier("email").withValue(timestamp, "gwu@usermail.example.com")
          .withQualifier("name").withValue(timestamp, "Garrett Wu")
          .withRow("aaron@usermail.example.com")
          .withFamily("info")
          .withQualifier("email").withValue(timestamp, "aaron@usermail.example.com")
          .withQualifier("name").withValue(timestamp, "Aaron Kimball")
          .withRow("christophe@usermail.example.com")
          .withFamily("info")
          .withQualifier("email")
          .withValue(timestamp, "christophe@usermail.example.com")
          .withQualifier("name").withValue(timestamp, "Christophe Bisciglia")
          .withRow("kiyan@usermail.example.com")
          .withFamily("info")
          .withQualifier("email").withValue(timestamp, "kiyan@usermail.example.com")
          .withQualifier("name").withValue(timestamp, "Kiyan Ahmadizadeh")
          .withRow("john.doe@gmail.com")
          .withFamily("info")
          .withQualifier("email").withValue(timestamp, "john.doe@gmail.com")
          .withQualifier("name").withValue(timestamp, "John Doe")
          .withRow("jane.doe@gmail.com")
          .withFamily("info")
          .withQualifier("email").withValue(timestamp, "jane.doe@gmail.com")
          .withQualifier("name").withValue(timestamp, "Jane Doe")
          .build();

    } finally {
      kiji.release();
    }
    mTableUri = KijiURI.newBuilder(mUri.toString()).withTableName("foo").build();
    assertTrue(mTableUri.isCassandra());
    LOG.info("Table URI = " + mTableUri);
  }

  @Test
  public void testGatherer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);


    final Path output = new Path(fs.getUri().toString(),
        String.format("/%s-%d/table-mr-output", getClass().getName(), System.currentTimeMillis()));

    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(conf)
        .withGatherer(TableMapper.class)
        .withReducer(TableReducer.class)
        .withInputTable(mTableUri)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(output, 16))
        .build();
    /*
    assertEquals(
        CassandraKijiTableInputFormat.class.getCanonicalName(),
        mrjob.getHadoopJob().getInputFormatClass().getCanonicalName()
    );
    */
    if (!mrjob.run()) {
      Assert.fail("MapReduce job failed");
    }
    // TODO: Actually check the output...
  }

  @Test
  public void testProducer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);


    final Path output = new Path(fs.getUri().toString(),
        String.format("/%s-%d/table-mr-output", getClass().getName(), System.currentTimeMillis()));

    final KijiMapReduceJob mrjob = KijiProduceJobBuilder.create()
        .withConf(conf)
        .withProducer(DomainProducer.class)
        .withInputTable(mTableUri)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTableUri))
        .build();
    if (!mrjob.run()) {
      Assert.fail("MapReduce job failed");
    }


  }
  /** Table mapper that processes Kiji rows and emits (key, value) pairs. */
  public static class TableMapper extends KijiGatherer<Text, Text> {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData input, GathererContext<Text, Text> context)
        throws IOException {
      try {
        LOG.info("Row key = " + input.getEntityId());
        final String email = input.getMostRecentValue("info", "email").toString();
        final String name = input.getMostRecentValue("info", "name").toString();
        context.write(new Text(email), new Text(name));
      } catch (NullPointerException npe) {
        LOG.info("Problem getting email and name from row data " + input);
      }
    }

  }

  /** Table reduces that processes (key, value) pairs and emits Kiji puts. */
  public static class TableReducer extends KijiReducer<Text, Text, Text, Text> {
    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
    /** {@inheritDoc} */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (Text text : values) {
        sb.append(text.toString());
      }
      String allKeys = sb.toString();
      context.write(key, new Text(allKeys));
    }
  }

  /** Producer that gets the domain from an e-mail address. */
  public static class DomainProducer extends KijiProducer {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    @Override
    public String getOutputColumn() {
      return "derived:domain";
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      try {
        LOG.info("Row key = " + input.getEntityId());
        final String email = input.getMostRecentValue("info", "email").toString();
        final String name = input.getMostRecentValue("info", "name").toString();

        // Split off the domain name from the e-mail address.
        if (email.contains("@")) {
          List<String> userAndDomain = Lists.newArrayList(Splitter.on("@").split(email));
          if (userAndDomain.size() == 2) {
            final String domain = userAndDomain.get(1);
            context.put(domain);
          }
        }
      } catch (NullPointerException npe) {
        LOG.info("Problem getting email and name from row data " + input);
      }
    }
  }
}
