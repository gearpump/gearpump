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

package org.apache.gearpump.jarstore

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.util.ServiceLoader

import com.typesafe.config.Config
import org.apache.gearpump.util.Util

import scala.collection.JavaConverters._

case class FilePath(path: String)

/**
 * JarStore is used to manage the upload/download of binary files,
 * like user submitted application jar.
 */
trait JarStore {
  /**
   * The scheme of the JarStore.
   * Like "hdfs" for HDFS file system, and "file" for a local
   * file system.
   */
  val scheme: String

  /**
   * Init the Jar Store.
   */
  def init(config: Config)

  /**
   * Creates the file on JarStore.
   *
   * @param fileName  name of the file to be created on JarStore.
   * @return OutputStream returns a stream into which the data can be written.
   */
  def createFile(fileName: String): OutputStream

  /**
   * Gets the InputStream to read the file
   *
   * @param fileName name of the file to be read on JarStore.
   * @return InputStream returns a stream from which the data can be read.
   */
  def getFile(fileName: String): InputStream
}

object JarStore {

  /**
   * Get a active JarStore by specifying a scheme.
   *
   * Please see config [[org.apache.gearpump.util.Constants#GEARPUMP_APP_JAR_STORE_ROOT_PATH]] for
   * more information.
   */
  private lazy val jarstores: List[JarStore] = {
    ServiceLoader.load(classOf[JarStore]).asScala.toList
  }

  def get(rootPath: String): JarStore = {
    val scheme = new URI(Util.resolvePath(rootPath)).getScheme
    jarstores.find(_.scheme == scheme).get
  }
}