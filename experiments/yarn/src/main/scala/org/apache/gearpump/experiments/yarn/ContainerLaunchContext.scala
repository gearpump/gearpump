/*-
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.experiments.yarn

import java.io.File
import java.nio.ByteBuffer

import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


object ContainerLaunchContext {
  val LOG: Logger = LogUtil.getLogger(getClass)
  var yarnConf: YarnConfiguration = _
  var appConfig: AppConfig = _

  private def getFs(yarnConf: YarnConfiguration) = FileSystem.get(yarnConf)

  private def getAppEnv(yarnConf: YarnConfiguration): Map[String, String] = {
    val classPaths = yarnConf.getStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(File.pathSeparator))
    val allPaths = Option(classPaths) match {
      case Some(paths) =>
        paths
      case None =>
        Array("")
    }
    allPaths :+ Environment.PWD.$()+File.separator+"*"+File.pathSeparator

    Map(Environment.CLASSPATH.name -> allPaths.reduceLeft((a,b) => {
      a + File.pathSeparator + b
    }))
  }

  private def  getAMLocalResourcesMap: Map[String, LocalResource] = {
    Try({
      val fs = getFs(yarnConf)
      val version = appConfig.getEnv("version")
      val hdfsRoot = appConfig.getEnv(HDFS_ROOT)
      Map(
        "pack" -> newYarnAppResource(fs, new Path(s"$hdfsRoot/$version.tar.gz"),
          LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC),
        "yarnConf" -> newYarnAppResource(fs, new Path(s"$hdfsRoot/conf"),
          LocalResourceType.FILE, LocalResourceVisibility.PUBLIC))
    }) match {
      case Success(map) =>
        map
      case Failure(throwable) =>
        Map.empty[String, LocalResource]
    }
  }

  private def newYarnAppResource(fs: FileSystem, path: Path,
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

  private def getToken: ByteBuffer = {
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData)
  }

  def apply(yc: YarnConfiguration, ac: AppConfig)(command: String): ContainerLaunchContext = {
    yarnConf = yc
    appConfig = ac
    val context = Records.newRecord(classOf[ContainerLaunchContext])
    context.setCommands(Seq(command))
    context.setEnvironment(getAppEnv(yarnConf))
    context.setTokens(getToken)
    context.setLocalResources(getAMLocalResourcesMap)
    context
  }

}

