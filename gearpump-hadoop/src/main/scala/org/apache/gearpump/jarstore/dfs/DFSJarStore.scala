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
package org.apache.gearpump.jarstore.dfs

import java.io.{InputStream, OutputStream}
import org.apache.gearpump.util.Constants
import org.apache.gearpump.jarstore.JarStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.typesafe.config.Config
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

/**
 * DFSJarStore store the uploaded jar on HDFS
 */
class DFSJarStore extends JarStore {
  private var rootPath: Path = null
  override val scheme: String = "hdfs"

  override def init(config: Config): Unit = {
    rootPath = new Path(config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH))
    val fs = rootPath.getFileSystem(new Configuration())
    if (!fs.exists(rootPath)) {
      fs.mkdirs(rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
  }

  /**
   * Creates the file on JarStore.
   *
   * @param fileName  name of the file to be created on JarStore.
   * @return OutputStream returns a stream into which the data can be written.
   */
  override def createFile(fileName: String): OutputStream = {
    val filePath = new Path(rootPath, fileName)
    val fs = filePath.getFileSystem(new Configuration())
    fs.create(filePath)
  }

  /**
   * Gets the InputStream to read the file
   *
   * @param fileName name of the file to be read on JarStore.
   * @return InputStream returns a stream from which the data can be read.
   */
  override def getFile(fileName: String): InputStream = {
    val filePath = new Path(rootPath, fileName)
    val fs = filePath.getFileSystem(new Configuration())
    fs.open(filePath)
  }
}
