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
package org.apache.gearpump.cluster.embedded

import com.typesafe.config.Config
import org.apache.gearpump.cluster.client.{ClientContext, RuntimeEnvironment}
import org.apache.gearpump.cluster.embedded.EmbeddedRuntimeEnvironment.EmbeddedClientContext

/**
 * The EmbeddedRuntimeEnvironment is initiated when user trying to launch their application
 * from IDE. It will create an embedded cluster and user's application will run in a single
 * local process.
 */
class EmbeddedRuntimeEnvironment extends RuntimeEnvironment {
  override def newClientContext(akkaConf: Config): ClientContext = {
    new EmbeddedClientContext(akkaConf)
  }
}

object EmbeddedRuntimeEnvironment {
  class EmbeddedClientContext private(cluster: EmbeddedCluster)
    extends ClientContext(cluster.config, cluster.system, cluster.master) {

    def this(akkaConf: Config) {
      this(new EmbeddedCluster(akkaConf))
    }

    override def close(): Unit = {
      super.close()
      cluster.stop()
    }
  }
}
