/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.gearpump.runtime;

import io.gearpump.Message;
import io.gearpump.streaming.partitioner.UnicastPartitioner;
import java.util.Arrays;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;

/** Partitions GroupByKey input by encoded Beam key bytes. */
@SuppressWarnings("unchecked")
public class BeamKeyPartitioner<K, V> implements UnicastPartitioner {

  private final Coder<K> keyCoder;

  public BeamKeyPartitioner(Coder<K> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public int getPartition(Message message, int partitionNum, int currentPartitionId) {
    try {
      WindowedValue<KV<K, V>> windowedValue = (WindowedValue<KV<K, V>>) message.value();
      byte[] encodedKey = CoderUtils.encodeToByteArray(keyCoder, windowedValue.getValue().getKey());
      return (Arrays.hashCode(encodedKey) & Integer.MAX_VALUE) % partitionNum;
    } catch (Exception e) {
      throw new RuntimeException("Failed to encode Beam key for partitioning", e);
    }
  }
}
