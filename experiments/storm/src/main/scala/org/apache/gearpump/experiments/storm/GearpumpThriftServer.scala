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

package org.apache.gearpump.experiments.storm

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import backtype.storm.generated._
import org.apache.gearpump.experiments.storm.Commands._
import org.apache.gearpump.util.Constants
import org.apache.thrift7.protocol.TBinaryProtocol
import org.apache.thrift7.server.{THsHaServer, TServer}
import org.apache.thrift7.transport.TNonblockingServerSocket

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object GearpumpThriftServer {
  val THRIFT_PORT = 6627
  val THRIFT_THREADS = 64
  val THRIFT_MAX_BUFFER_SIZE = 1048576
  implicit val timeOut = akka.util.Timeout(3, TimeUnit.SECONDS)

  private def createServer(handler: ActorRef): TServer = {
    val serverTransport = new TNonblockingServerSocket(THRIFT_PORT)
    val args = new THsHaServer.Args(serverTransport)
    args.workerThreads(THRIFT_THREADS)
      .protocolFactory(new TBinaryProtocol.Factory(false, true, THRIFT_MAX_BUFFER_SIZE))
      .processor(new Nimbus.Processor[GearpumpNimbus](new GearpumpNimbus(handler)))
    new THsHaServer(args)
  }

  def apply(handler: ActorRef): GearpumpThriftServer = {
    new GearpumpThriftServer(createServer(handler))
  }

  class GearpumpNimbus(handler: ActorRef) extends Nimbus.Iface {

    override def submitTopology(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology): Unit = {
      handler ! Submit(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE))
    }

    override def killTopologyWithOpts(name: String, options: KillOptions): Unit = {
      handler ! Kill(name, options)
    }

    override def submitTopologyWithOpts(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology, options: SubmitOptions): Unit = {
      handler ! Submit(name, uploadedJarLocation, jsonConf, topology, options)
    }

    override def uploadChunk(location: String, chunk: ByteBuffer): Unit = {
    }

    override def getNimbusConf: String = {
      throw new UnsupportedOperationException
    }

    override def getTopology(id: String): StormTopology = {
      Await.result((handler ? GetTopology(id)).asInstanceOf[Future[StormTopology]], 10 seconds)
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

    override def killTopology(name: String): Unit = killTopologyWithOpts(name, new KillOptions())

    override def downloadChunk(id: String): ByteBuffer = {
      throw new UnsupportedOperationException
    }

    override def beginFileUpload(): String = {
      "local thrift server"
    }

    override def getClusterInfo: ClusterSummary = {
      Await.result((handler ? GetClusterInfo).asInstanceOf[Future[ClusterSummary]], 10 seconds)
    }

    override def finishFileUpload(location: String): Unit = {
    }
  }
}

class GearpumpThriftServer(server: TServer) extends Thread {

  override def run(): Unit = {
    server.serve()
  }

  def close(): Unit = {
    server.stop()
  }
}
