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

package io.gearpump.experiments.yarn.glue

import java.io.File
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem => YarnFileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.slf4j.Logger

import io.gearpump.util.LogUtil

private[glue]
object ContainerLaunchContext {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def apply(yarnConf: YarnConfiguration, command: String, packagePath: String, configPath: String)
    : ContainerLaunchContext = {
    val context = Records.newRecord(classOf[ContainerLaunchContext])
    context.setCommands(Seq(command).asJava)
    context.setEnvironment(getAppEnv(yarnConf).asJava)
    context.setTokens(getToken(yarnConf, packagePath, configPath))
    context.setLocalResources(getAMLocalResourcesMap(yarnConf, packagePath, configPath).asJava)
    context
  }

  private def getFs(yarnConf: YarnConfiguration) = YarnFileSystem.get(yarnConf)

  private def getAppEnv(yarnConf: YarnConfiguration): Map[String, String] = {
    val classPaths = yarnConf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(File.pathSeparator))
    val allPaths = Option(classPaths).getOrElse(Array(""))

    allPaths :+ Environment.PWD.$() + File.separator + "*" + File.pathSeparator

    Map(Environment.CLASSPATH.name -> allPaths.map(_.trim).mkString(File.pathSeparator))
  }

  private def getAMLocalResourcesMap(
      yarnConf: YarnConfiguration, packagePath: String, configPath: String)
    : Map[String, LocalResource] = {
    val fs = getFs(yarnConf)

    Map(
      "pack" -> newYarnAppResource(fs, new Path(packagePath),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION),
      "conf" -> newYarnAppResource(fs, new Path(configPath),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION))
  }

  private def newYarnAppResource(
      fs: YarnFileSystem, path: Path,
      resourceType: LocalResourceType, vis: LocalResourceVisibility): LocalResource = {
    val qualified = fs.makeQualified(path)
    val status = fs.getFileStatus(qualified)
    val resource = Records.newRecord(classOf[LocalResource])
    resource.setType(resourceType)
    resource.setVisibility(vis)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified))
    resource.setTimestamp(status.getModificationTime)
    resource.setSize(status.getLen)
    resource
  }

  private def getToken(yc: YarnConfiguration, packagePath: String, configPath: String)
    : ByteBuffer = {
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    val dirs = Array(new Path(packagePath), new Path(configPath))
    TokenCache.obtainTokensForNamenodes(credentials, dirs, yc)
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData)
  }
}