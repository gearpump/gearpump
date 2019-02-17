/*
 * Licensed under the Apache License, Version 2.0 (the
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
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.javaapi.StreamApplication;
import io.gearpump.streaming.partitioner.HashPartitioner;
import io.gearpump.streaming.partitioner.Partitioner;

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
    graph.addVertexAndEdge(split, partitioner, sum);

    UserConfig conf = UserConfig.empty();
    StreamApplication app = new StreamApplication("wordcountJava", conf, graph);
    ClientContext masterClient = ClientContext.apply(akkaConf);
    masterClient.submit(app);

    masterClient.close();
  }
}