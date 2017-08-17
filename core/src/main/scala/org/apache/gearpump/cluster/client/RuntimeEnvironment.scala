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

package org.apache.gearpump.cluster.client

import com.typesafe.config.Config
import org.apache.gearpump.cluster.client.RuntimeEnvironment.RemoteClientContext
import org.apache.gearpump.cluster.embedded.EmbeddedRuntimeEnvironment

/**
 * The RuntimeEnvironment is the context decides where an application is submitted to.
 */
abstract class RuntimeEnvironment {
  def newClientContext(akkaConf: Config): ClientContext
}

/**
 * Usually RemoteRuntimeEnvironment is the default enviroment when user using command line
 * to submit application. It will connect to the right remote Master.
 */
class RemoteRuntimeEnvironment extends RuntimeEnvironment {
  override def newClientContext(akkaConf: Config): ClientContext = {
    new RemoteClientContext(akkaConf)
  }
}

object RuntimeEnvironment {
  private var envInstance: RuntimeEnvironment = _

  class RemoteClientContext(akkaConf: Config) extends ClientContext(akkaConf, null, null)

  def get() : RuntimeEnvironment = {
    Option(envInstance).getOrElse(new EmbeddedRuntimeEnvironment)
  }

  def newClientContext(akkaConf: Config): ClientContext = {
    get().newClientContext(akkaConf)
  }

  def setRuntimeEnv(env: RuntimeEnvironment): Unit = {
    envInstance = env
  }
}
