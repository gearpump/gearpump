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
package org.apache.gearpump.appstorage.hadoopfs

import java.io.File

import org.apache.gearpump.appstorage.{AppInfo, JarStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
 * Using Hadoop file system to store jar files
 */
class HadoopFSJarStore(appInfo: AppInfo, jarDir: String, hadoopConf: Configuration) extends JarStore {
  val fs = FileSystem.get(hadoopConf)
  val remotePath = new Path(jarDir)

  override def copyFromLocal(localFile: String): Unit = {
    val localPath = new Path(localFile)
    fs.copyFromLocalFile(false, localPath, remotePath)
  }

  override def copyToLocal(remoteFile: String, localPath: String): Unit = {
    val local = new Path(localPath)
    val remote = new Path(s"$remotePath/$remoteFile")
    fs.copyToLocalFile(remote, local)
  }
}
