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

package org.apache.gearpump.jarstore.dfs

import java.io.File

import akka.actor.{Actor, Stash}
import org.apache.gearpump.cluster.ClientToMaster.GetJarFileContainer
import org.apache.gearpump.jarstore.JarFileContainer
import org.apache.gearpump.util.{Constants, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.slf4j.Logger

class DFSJarStore(rootDir : String) extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val host = context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)

  val rootPath = new Path(rootDir)
  val fs = rootPath.getFileSystem(new Configuration())
  if (!fs.exists(rootPath)) {
    fs.mkdirs(rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
  }

  def receive : Receive = {
    case GetJarFileContainer =>
      val name = Math.abs((new java.util.Random()).nextLong()).toString
      val target = new Path(rootPath, name)
      sender ! new DFSJarFileContainer(target.toString)
  }
}

/**
 * Use DFS to store application jar files
 */
class DFSJarFileContainer(fileURI : String) extends JarFileContainer {

  private def LOG: Logger = LogUtil.getLogger(getClass)

  override def copyFromLocal(localFile: File): Unit = {

    LOG.info(s"Copying from local file: ${localFile.getAbsolutePath} to $fileURI")

    val filePath = new Path(fileURI)
    val fs = filePath.getFileSystem(new Configuration())
    fs.copyFromLocalFile(new Path(localFile.toURI().toString), filePath)
  }

  override def copyToLocalFile(localFile: File): Unit = {


    LOG.info(s"Copying to local file: ${localFile.getAbsolutePath} from $fileURI")

    val filePath = new Path(fileURI)
    val fs = filePath.getFileSystem(new Configuration())
    val target = new Path(localFile.toURI().toString)
    fs.copyToLocalFile(filePath, target)
  }
}