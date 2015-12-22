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
package io.gearpump.jarstore

import java.io.File
import java.net.URI
import java.util.ServiceLoader

import akka.actor.{ActorSystem, ActorRefFactory}
import com.typesafe.config.Config
import io.gearpump.util.{Constants, Util}

import scala.collection.JavaConverters._

case class FilePath(path: String)

/**
 * JarStoreService is used to manage the upload/download of binary files,
 * like user submitted application jar.
 */
trait JarStoreService {
  /**
   * The scheme of the JarStoreService.
   * Like "hdfs" for HDFS file system, and "file" for a local
   * file system.
   */
  val scheme : String

  /**
   * Init the Jar Store.
   */
  def init(config: Config, system: ActorSystem)

  /**
    * This function will copy the local file to the remote JarStore, called from client side.
   * @param localFile The local file
   */
  def copyFromLocal(localFile: File): FilePath

  /**
    * This function will copy the remote file to local file system, called from client side.
   * @param localFile The destination of file path
   * @param remotePath The remote file path from JarStore
   */
  def copyToLocalFile(localFile: File, remotePath: FilePath)
}

object JarStoreService {

  /**
   * Get a active JarStoreService by specifying a scheme.
   * Please see config [[Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH]] for more
   * information.
   */
  def get(config: Config): JarStoreService = {
    val jarStoreRootPath = config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH)
    get(jarStoreRootPath)
  }

  private lazy val jarstoreServices: List[JarStoreService] = {
    ServiceLoader.load(classOf[JarStoreService]).asScala.toList
  }

  private def get(rootPath: String): JarStoreService = {
    val scheme = new URI(Util.resolvePath(rootPath)).getScheme
    jarstoreServices.find(_.scheme == scheme).get
  }
}