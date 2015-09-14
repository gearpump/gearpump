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


package io.gearpump.streaming.javaapi;

import akka.actor.ActorSystem;
import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.partitioner.Partitioner;
import io.gearpump.util.Graph;

public class StreamApplication<T extends Processor<Task>, P extends Partitioner>{
  private ActorSystem _system;
  private Graph<T, P> _graph;
  private String _name;
  private UserConfig _conf;

  public StreamApplication(String name, UserConfig conf, ActorSystem system) {
    this._system = system;
    this._graph = Graph.<T, P>empty();
    this._name = name;
    this._conf = conf;
  }

  public void addVertex(T vertex) {
    _graph.addVertex(vertex);
  }

  public void addEdge(T node1, P edge, T node2) {
    _graph.addEdge(node1, edge, node2);
  }

  public int submit() {
    io.gearpump.streaming.StreamApplication app = io.gearpump.streaming.StreamApplication.<T, P>apply(_name, _graph, _conf);
    ClientContext ctx = ClientContext.apply(_system.settings().config(), _system, null);
    return ctx.submit(app);
  }
}