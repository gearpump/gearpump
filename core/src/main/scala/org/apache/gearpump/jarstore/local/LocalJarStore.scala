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

import java.io._

import com.typesafe.config.Config
import org.apache.gearpump.jarstore.JarStore
import org.apache.gearpump.util.{LogUtil, FileUtils, Constants}
import org.slf4j.Logger

/**
 * LocalJarStore store the uploaded jar on local disk.
 */
class LocalJarStore extends JarStore {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private var rootPath: String = null
  override val scheme: String = "file"

  class ClosedInputStream extends InputStream {
    override def read(): Int = -1
  }

  override def init(config: Config): Unit = {
    rootPath = config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH)
    FileUtils.forceMkdir(new File(rootPath))
  }

  /**
   * Creates the file on JarStore.
   *
   * @param fileName  name of the file to be created on JarStore.
   * @return OutputStream returns a stream into which the data can be written.
   */
  override def createFile(fileName: String): OutputStream = {
    val localFile = new File(rootPath, fileName)
    new FileOutputStream(localFile)
  }

  /**
   * Gets the InputStream to read the file
   *
   * @param fileName name of the file to be read on JarStore.
   * @return InputStream returns a stream from which the data can be read.
   */
  override def getFile(fileName: String): InputStream = {
    val localFile = new File(rootPath, fileName)
    val is = try {
      new FileInputStream(localFile)
    } catch {
      case ex: Exception =>
        LOG.error(s"Fetch file $fileName failed: ${ex.getStackTrace}")
        new ClosedInputStream
    }
    is
  }
}