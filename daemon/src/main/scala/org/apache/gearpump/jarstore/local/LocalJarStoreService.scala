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
package org.apache.gearpump.jarstore.local

import java.io.File
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.typesafe.config.Config
import org.slf4j.Logger

import org.apache.gearpump.cluster.ClientToMaster.{GetJarStoreServer, JarStoreServerAddress}
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.jarstore.{FilePath, JarStoreService}
import org.apache.gearpump.util._

/**
 * LocalJarStoreService store the uploaded jar on local disk.
 */
class LocalJarStoreService extends JarStoreService {
  private def LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Constants.FUTURE_TIMEOUT
  private var system: akka.actor.ActorSystem = null
  private var master: ActorRef = null
  private implicit def dispatcher: ExecutionContext = system.dispatcher

  override val scheme: String = "file"

  override def init(config: Config, system: ActorSystem): Unit = {
    this.system = system
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS)
      .asScala.flatMap(Util.parseHostList)
    master = system.actorOf(MasterProxy.props(masters), s"masterproxy${Util.randInt()}")
  }

  private lazy val client = (master ? GetJarStoreServer).asInstanceOf[Future[JarStoreServerAddress]]
    .map { address =>
      val client = new FileServer.Client(system, address.url)
      client
    }

  /**
   * This function will copy the remote file to local file system, called from client side.
   *
   * @param localFile The destination of file path
   * @param remotePath The remote file path from JarStore
   */
  override def copyToLocalFile(localFile: File, remotePath: FilePath): Unit = {
    LOG.info(s"Copying to local file: ${localFile.getAbsolutePath} from $remotePath")
    val future = client.flatMap(_.download(remotePath, localFile))
    Await.ready(future, Duration(60, TimeUnit.SECONDS))
  }

  /**
   * This function will copy the local file to the remote JarStore, called from client side.
   * @param localFile The local file
   */
  override def copyFromLocal(localFile: File): FilePath = {
    val future = client.flatMap(_.upload(localFile))
    Await.result(future, Duration(60, TimeUnit.SECONDS))
  }
}