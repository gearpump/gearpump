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

package org.apache.gearpump.experiments.storm.util

import backtype.storm.generated.StormTopology
import backtype.storm.testing.{TestGlobalCount, TestWordCounter, TestWordSpout}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

object TopologyUtil {

  def getTestTopology: StormTopology = {
    val topologyBuilder = new TopologyBuilder
    topologyBuilder.setSpout("1", new TestWordSpout(true), 5)
    topologyBuilder.setSpout("2", new TestWordSpout(true), 3)
    topologyBuilder.setBolt("3", new TestWordCounter(), 3)
      .fieldsGrouping("1", new Fields("word"))
      .fieldsGrouping("2", new Fields("word"))
    topologyBuilder.setBolt("4", new TestGlobalCount()).globalGrouping("1")
    topologyBuilder.createTopology()
  }
}
