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

import backtype.storm.generated.Nimbus
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.thrift7.protocol.TBinaryProtocol
import org.apache.thrift7.server.{THsHaServer, TServer}
import org.apache.thrift7.transport.TNonblockingServerSocket

object GearpumpThriftServer {
  val THRIFT_PORT = 6627
  val THRIFT_THREADS = 64
  val THRIFT_MAX_BUFFER_SIZE = 1048576

  def apply(clientContext: ClientContext): GearpumpThriftServer = new GearpumpThriftServer(clientContext)
}

class GearpumpThriftServer(clientContext: ClientContext) extends Thread {
  import org.apache.gearpump.experiments.storm.GearpumpThriftServer._

  private val server: TServer = createServer

  override def run(): Unit = {
    server.serve()
  }

  private def createServer: TServer = {
    val serverTransport = new TNonblockingServerSocket(THRIFT_PORT)
    val args = new THsHaServer.Args(serverTransport)
    args.workerThreads(THRIFT_THREADS)
      .protocolFactory(new TBinaryProtocol.Factory(false, true, THRIFT_MAX_BUFFER_SIZE))
      .processor(new Nimbus.Processor[GearpumpNimbus](new GearpumpNimbus(clientContext)))
    new THsHaServer(args)
  }

  def close(): Unit = {
    server.stop()
  }

}
