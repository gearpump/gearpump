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

package io.gearpump.streaming.javaapi;

import akka.actor.ActorSystem;
import io.gearpump.cluster.Application;
import io.gearpump.cluster.ApplicationMaster;
import io.gearpump.cluster.UserConfig;

/**
 * Java version of StreamApplication.
 *
 * Also see {@link io.gearpump.streaming.StreamApplication}
 */
public class StreamApplication implements Application {
  private io.gearpump.streaming.StreamApplication app;
  /**
   * Creates a streaming application
   *
   * @param name  Name of the application
   * @param conf  User configuration
   * @param graph The DAG
   */
  public StreamApplication(String name, UserConfig conf, Graph graph) {
    //by pass the tricky type check in scala 2.10
    io.gearpump.util.Graph untypedGraph = graph;
    this.app = io.gearpump.streaming.StreamApplication.apply(
      name, untypedGraph, conf);
  }

  @Override
  public String name() {
    return app.name();
  }

  @Override
  public UserConfig userConfig(ActorSystem system) {
    return app.userConfig(system);
  }

  @Override
  public Class<? extends ApplicationMaster> appMaster() {
    return app.appMaster();
  }
}