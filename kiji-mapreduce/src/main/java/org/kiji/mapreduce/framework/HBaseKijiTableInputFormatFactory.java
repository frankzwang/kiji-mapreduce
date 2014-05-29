/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.util.Map;

import com.google.common.base.Preconditions;

import org.kiji.delegation.Priority;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * Factory for getting instances of KijiTableInputFormat (for tables backed by HBase).
 */
public class HBaseKijiTableInputFormatFactory implements KijiTableInputFormatFactory {
  /** {@inheritDoc} */
  @Override
  public KijiTableInputFormat getInputFormat() {
    return new HBaseKijiTableInputFormat();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    Preconditions.checkArgument(runtimeHints.containsKey(Kiji.KIJI_TYPE_KEY));
    if (runtimeHints.get(Kiji.KIJI_TYPE_KEY).equals(KijiURI.TYPE_HBASE)) {
      return Priority.NORMAL;
    } else {
      return Priority.DISABLED;
    }
  }
}
