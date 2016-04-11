/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.examples.wordcountjava;

import com.typesafe.config.Config;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.cluster.embedded.EmbeddedCluster;
import io.gearpump.partitioner.HashPartitioner;
import io.gearpump.partitioner.Partitioner;
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.javaapi.StreamApplication;

/** Java version of WordCount with Processor Graph API */
public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {

    // For split task, we config to create two tasks
    int splitTaskNumber = 2;
    Processor split = new Processor(Split.class).withParallelism(splitTaskNumber);

    // For sum task, we have two summer.
    int sumTaskNumber = 2;
    Processor sum = new Processor(Sum.class).withParallelism(sumTaskNumber);

    // construct the graph
    Graph graph = new Graph();
    graph.addVertex(split);
    graph.addVertex(sum);

    Partitioner partitioner = new HashPartitioner();
    graph.addEdge(split, partitioner, sum);

    UserConfig conf = UserConfig.empty();
    StreamApplication app = new StreamApplication("wordcountJava", conf, graph);

    EmbeddedCluster localCluster = null;

    Boolean debugMode = System.getProperty("DEBUG") != null;

    if (debugMode) {
      localCluster = new EmbeddedCluster(akkaConf);
      localCluster.start();
    }

    ClientContext masterClient = null;

    if (localCluster != null) {
      masterClient = localCluster.newClientContext();
    } else {
      // create master client
      // It will read the master settings under gearpump.cluster.masters
      masterClient = new ClientContext(akkaConf);
    }

    masterClient.submit(app);

    if (debugMode) {
      Thread.sleep(30 * 1000); // sleep for 30 seconds.
    }

    masterClient.close();

    if (localCluster != null) {
      localCluster.stop();
    }
  }
}