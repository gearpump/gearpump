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

package io.gearpump.experiments.storm

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.{HashMap => JHashMap, Map => JMap}

import akka.actor.ActorRef
import backtype.storm.Config
import backtype.storm.generated._
import backtype.storm.security.auth.{ThriftConnectionType, ThriftServer}
import backtype.storm.utils.Utils
import io.gearpump.experiments.storm.Commands.{GetClusterInfo, _}
import io.gearpump.experiments.storm.util.StormUtil
import io.gearpump.util.ActorUtil.askActor

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object GearpumpThriftServer {
  val THRIFT_PORT = StormUtil.getThriftPort
  implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)

  private def createServer(handler: ActorRef): ThriftServer = {
    val processor = new Nimbus.Processor[GearpumpNimbus](new GearpumpNimbus(handler))
    val connectionType = ThriftConnectionType.NIMBUS
    val config = Utils.readDefaultConfig().asInstanceOf[JMap[AnyRef, AnyRef]]
    config.put(Config.NIMBUS_THRIFT_PORT, s"$THRIFT_PORT")
    new ThriftServer(config, processor, connectionType)
  }

  def apply(handler: ActorRef): GearpumpThriftServer = {
    new GearpumpThriftServer(createServer(handler))
  }

  class GearpumpNimbus(handler: ActorRef) extends Nimbus.Iface {

    override def submitTopology(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology): Unit = {
      val ask = askActor(handler, Submit(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE)))
      Await.result(ask, 30 seconds)
    }

    override def killTopologyWithOpts(name: String, options: KillOptions): Unit = {
      Await.result(askActor(handler,Kill(name, options)), 10 seconds)
    }

    override def submitTopologyWithOpts(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology, options: SubmitOptions): Unit = {
      Await.result(askActor(handler,Submit(name, uploadedJarLocation, jsonConf, topology, options)), 10 seconds)
    }

    override def uploadChunk(location: String, chunk: ByteBuffer): Unit = {
    }

    override def getNimbusConf: String = {
      throw new UnsupportedOperationException
    }

    override def getTopology(id: String): StormTopology = {
      Await.result(askActor[StormTopology](handler, GetTopology(id)), 10 seconds)
    }

    override def getTopologyConf(id: String): String = {
      throw new UnsupportedOperationException
    }

    override def beginFileDownload(file: String): String = {
      throw new UnsupportedOperationException
    }

    override def getUserTopology(id: String): StormTopology = getTopology(id)

    override def activate(name: String): Unit = {
      throw new UnsupportedOperationException
    }

    override def rebalance(name: String, options: RebalanceOptions): Unit = {
      throw new UnsupportedOperationException
    }

    override def deactivate(name: String): Unit = {
      throw new UnsupportedOperationException
    }

    override def getTopologyInfo(id: String): TopologyInfo = {
      throw new UnsupportedOperationException
    }

    override def getTopologyInfoWithOpts(s: String, getInfoOptions: GetInfoOptions): TopologyInfo = {
      throw new UnsupportedOperationException
    }

    override def killTopology(name: String): Unit = killTopologyWithOpts(name, new KillOptions())

    override def downloadChunk(id: String): ByteBuffer = {
      throw new UnsupportedOperationException
    }

    override def beginFileUpload(): String = {
      "local thrift server"
    }

    override def getClusterInfo: ClusterSummary = {
      Await.result(askActor[ClusterSummary](handler, GetClusterInfo), 10 seconds)
    }

    override def finishFileUpload(location: String): Unit = {
    }

    override def uploadNewCredentials(s: String, credentials: Credentials): Unit = {
      throw new UnsupportedOperationException
    }
  }
}

class GearpumpThriftServer(server: ThriftServer) extends Thread {

  override def run(): Unit = {
    server.serve()
  }

  def close(): Unit = {
    server.stop()
  }
}