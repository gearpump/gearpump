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

package org.apache.gearpump.experiments.storm.partitioner

import backtype.storm.tuple.Fields
import org.apache.gearpump.Message
import org.apache.gearpump.experiments.storm.util.StormTuple
import org.apache.gearpump.partitioner.Partitioner
import scala.collection.JavaConverters._


private[storm] class FieldsGroupingPartitioner(outFields: Fields, groupFields: Fields) extends Partitioner {
  override def getPartition(msg: Message, partitionNum: Int): Int = {
    val values = msg.msg.asInstanceOf[StormTuple].values
    val hash = outFields.select(groupFields, values.asJava).hashCode()
    hash % partitionNum
  }
}
