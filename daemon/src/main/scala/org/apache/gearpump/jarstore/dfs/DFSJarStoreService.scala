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

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.slf4j.Logger

import org.apache.gearpump.jarstore.{FilePath, JarStoreService}
import org.apache.gearpump.util.{Constants, LogUtil}

/**
 * DFSJarStoreService store the uploaded jar on HDFS
 */
class DFSJarStoreService extends JarStoreService {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private var rootPath: Path = null

  override val scheme: String = "hdfs"

  override def init(config: Config, actorRefFactory: ActorSystem): Unit = {
    rootPath = new Path(config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH))
    val fs = rootPath.getFileSystem(new Configuration())
    if (!fs.exists(rootPath)) {
      fs.mkdirs(rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
  }

  /**
   * This function will copy the remote file to local file system, called from client side.
   *
   * @param localFile The destination of file path
   * @param remotePath The remote file path from JarStore
   */
  override def copyToLocalFile(localFile: File, remotePath: FilePath): Unit = {
    val filePath = new Path(rootPath, remotePath.path)
    val fs = filePath.getFileSystem(new Configuration())
    LOG.info(s"Copying to local file: ${localFile.getAbsolutePath} from ${filePath.toString}")
    val target = new Path(localFile.toURI().toString)
    fs.copyToLocalFile(filePath, target)
  }

  /**
   * This function will copy the local file to the remote JarStore, called from client side.
   *
   * @param localFile The local file
   */
  override def copyFromLocal(localFile: File): FilePath = {
    val remotePath = FilePath(Math.abs(new java.util.Random().nextLong()).toString)
    val filePath = new Path(rootPath, remotePath.path)
    val fs = filePath.getFileSystem(new Configuration())
    LOG.info(s"Copying from local file: ${localFile.getAbsolutePath} to ${filePath.toString}")
    fs.copyFromLocalFile(new Path(localFile.toURI.toString), filePath)
    remotePath
  }
}
