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

package org.apache.gearpump.sql.utils;

import com.typesafe.config.Config;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;

public class GearConfiguration {

  private Config akkaConf;
  private ClientContext context;
  public static JavaStreamApp app;

  public void defaultConfiguration() {
    setAkkaConf(ClusterConfig.defaultConfig());
    setContext(ClientContext.apply(akkaConf));
  }

  public void ConfigJavaStreamApp() {
    app = new JavaStreamApp("JavaDSL", context, UserConfig.empty());
  }

  public void setAkkaConf(Config akkaConf) {
    this.akkaConf = akkaConf;
  }

  public void setContext(ClientContext context) {
    this.context = context;
  }
}
