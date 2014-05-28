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

package org.kiji.mapreduce.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiPutter;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * Kiji context that writes cells to a configured output table.
 *
 * <p> Implemented as direct writes sent to the Cassandra table.
 *
 * <p> Using this table writer context in a MapReduce is strongly discouraged :
 * pushing a lot of data into a running Cassandra instance may trigger compaction events
 * and cause the Cassandra instance to slow down.
 */
@ApiAudience.Private
public final class DirectCassandraKijiTableWriterContext
    extends InternalKijiContext
    implements KijiTableContext {

  private final Kiji mKiji;
  private final KijiTable mTable;
  private final KijiPutter mPutter;
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Constructs a new context that can write cells directly to a Kiji table.
   *
   * @param hadoopContext is the Hadoop {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}
   *     that will be used to perform the writes.
   * @throws java.io.IOException on I/O error.
   */
  public DirectCassandraKijiTableWriterContext(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    super(hadoopContext);
    final Configuration conf = new Configuration(hadoopContext.getConfiguration());
    final KijiURI outputURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();
    mKiji = Kiji.Factory.open(outputURI);
    mTable = mKiji.openTable(outputURI.getTable());
    mPutter = mTable.getWriterFactory().openBufferedWriter();
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());
  }

  /**
   * Creates a new context that can write cells directly to a Kiji table.
   *
   * @param hadoopContext is the Hadoop {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}
   *     that will be used to perform the writes.
   * @return a new context that can write cells directly to a Kiji table.
   * @throws java.io.IOException if there is an I/O error.
   */
  public static DirectCassandraKijiTableWriterContext
      create(TaskInputOutputContext<?, ?, ?, ?> hadoopContext) throws IOException {
    return new DirectCassandraKijiTableWriterContext(hadoopContext);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    mPutter.put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    mPutter.put(entityId, family, qualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public EntityIdFactory getEntityIdFactory() {
    return mEntityIdFactory;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... components) {
    return mEntityIdFactory.getEntityId(components);
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    mPutter.flush();
    super.flush();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    ResourceUtils.closeOrLog(mPutter);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
    super.close();
  }
}
