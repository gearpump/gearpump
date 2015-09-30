/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.jarstore.local

import java.io.File
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem, ActorRefFactory}
import io.gearpump.cluster.ClientToMaster.{JarStoreServerAddress, GetJarStoreServer}
import io.gearpump.cluster.master.MasterProxy
import com.typesafe.config.Config
import io.gearpump.jarstore.{FilePath, JarStoreService}
import io.gearpump.util._
import scala.collection.JavaConversions._
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}

class LocalJarStoreService extends JarStoreService{
  private def LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Constants.FUTURE_TIMEOUT
  private var system : akka.actor.ActorSystem = null
  private var master : ActorRef = null
  private implicit def dispatcher: ExecutionContext = system.dispatcher

  override val scheme: String = "file"

  override def init(config: Config, system: ActorSystem): Unit = {
    this.system = system
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
    master = system.actorOf(MasterProxy.props(masters), s"masterproxy${Util.randInt}")
  }

  private lazy val client = (master ? GetJarStoreServer).asInstanceOf[Future[JarStoreServerAddress]].map { address =>
    val client = new FileServer.Client(system, address.url)
    client
  }

  /**
   * This function will copy the remote file to local file system, called from client side.
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