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
package io.gearpump.jarstore.dfs

import java.io.File
import com.typesafe.config.Config
import io.gearpump.util.Constants
import org.apache.hadoop.fs.Path
import io.gearpump.jarstore.{ConfigRequired, FilePath, JarStoreService}
import io.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import io.gearpump.util.Constants
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.slf4j.Logger

class DFSJarStoreService extends JarStoreService with ConfigRequired{
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private var rootPath: Path = null

  override val scheme: String = "hdfs"

  /**
   * Use config to initiate the JarStoreService
   * @param config
   */
  override def init(config: Config): Unit = {
    rootPath = new Path(config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH))
    val fs = rootPath.getFileSystem(new Configuration())
    if (!fs.exists(rootPath)) {
      fs.mkdirs(rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
  }

  /**
    * This function will copy the remote file to local file system, called from client side.
   * @param localFile The destination of file path
   * @param remotePath The remote file path from JarStore
   */
  override def copyToLocalFile(localFile: File, remotePath: FilePath): Unit = {
    LOG.info(s"Copying to local file: ${localFile.getAbsolutePath} from ${remotePath}")
    val filePath = new Path(rootPath, remotePath.path)
    val fs = filePath.getFileSystem(new Configuration())
    val target = new Path(localFile.toURI().toString)
    fs.copyToLocalFile(filePath, target)
  }

  /**
   * This function will copy the local file to the remote JarStore, called from client side.
   * @param localFile The local file
   * @param remotePath The path on JarStore
   */
  override def copyFromLocal(localFile: File, remotePath: FilePath): Unit = {
    LOG.info(s"Copying from local file: ${localFile.getAbsolutePath} to ${remotePath}")
    val filePath = new Path(rootPath, remotePath.path)
    val fs = filePath.getFileSystem(new Configuration())
    fs.copyFromLocalFile(new Path(localFile.toURI.toString), filePath)
  }
}
