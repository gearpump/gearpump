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
package org.apache.gearpump.streaming.hadoop.lib

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.util.{Constants, FileUtils}

private[hadoop] object HadoopUtil {

  def getOutputStream(path: Path, hadoopConfig: Configuration): FSDataOutputStream = {
    val dfs = getFileSystemForPath(path, hadoopConfig)
    val stream: FSDataOutputStream = {
      if (dfs.isFile(path)) {
        dfs.append(path)
      } else {
        dfs.create(path)
      }
    }
    stream
  }

  def getInputStream(path: Path, hadoopConfig: Configuration): FSDataInputStream = {
    val dfs = getFileSystemForPath(path, hadoopConfig)
    val stream = dfs.open(path)
    stream
  }

  def getFileSystemForPath(path: Path, hadoopConfig: Configuration): FileSystem = {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    val fs = path.getFileSystem(hadoopConfig)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
  }

  def login(userConfig: UserConfig, configuration: Configuration): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val principal = userConfig.getString(Constants.GEARPUMP_KERBEROS_PRINCIPAL)
      val keytabContent = userConfig.getBytes(Constants.GEARPUMP_KEYTAB_FILE)
      if (principal.isEmpty || keytabContent.isEmpty) {
        val errorMsg = s"HDFS is security enabled, user should provide kerberos principal in " +
          s"${Constants.GEARPUMP_KERBEROS_PRINCIPAL} " +
          s"and keytab file in ${Constants.GEARPUMP_KEYTAB_FILE}"
        throw new Exception(errorMsg)
      }
      val keytabFile = File.createTempFile("login", ".keytab")
      FileUtils.writeByteArrayToFile(keytabFile, keytabContent.get)
      keytabFile.setExecutable(false)
      keytabFile.setWritable(false)
      keytabFile.setReadable(true, true)

      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytabFile.getAbsolutePath)
      keytabFile.delete()
    }
  }
}
