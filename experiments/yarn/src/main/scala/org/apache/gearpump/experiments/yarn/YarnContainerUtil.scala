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

import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger
import org.apache.hadoop.yarn.util.Apps
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.io.DataOutputBuffer
import java.nio.ByteBuffer


object YarnContainerUtil {
  val LOG: Logger = LogUtil.getLogger(getClass)
  
  //TODO: move to config
  val HDFS_JARS_DIR = "/user/gearpump/jars/"
  
  def getFs(yarnConf: YarnConfiguration) = FileSystem.get(yarnConf)  
  
  def getHdfsPath(yarnConf: YarnConfiguration) = new Path(getFs(yarnConf).getHomeDirectory, HDFS_JARS_DIR)

  def getAppEnv(yarnConf: YarnConfiguration): Map[String, String] = {
    val appMasterEnv = new java.util.HashMap[String,String]
    for (
      c <- yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(","))
    ) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim(), File.pathSeparator)
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$()+File.separator+"*", File.pathSeparator)
    appMasterEnv.toMap
  }

  def getAMLocalResourcesMap(yarnConf: YarnConfiguration): Map[String, LocalResource] = {
      val hdfsPath = getHdfsPath(yarnConf)
      getFs(yarnConf).listStatus(hdfsPath).map(fileStatus => {
      val localResouceFile = Records.newRecord(classOf[LocalResource])
      val path = ConverterUtils.getYarnUrlFromPath(fileStatus.getPath)
      LOG.info(s"local resource path=${path.getFile}")
      localResouceFile.setResource(path)
      localResouceFile.setType(LocalResourceType.FILE)
      localResouceFile.setSize(fileStatus.getLen)
      localResouceFile.setTimestamp(fileStatus.getModificationTime)
      localResouceFile.setVisibility(LocalResourceVisibility.APPLICATION)
      fileStatus.getPath.getName -> localResouceFile
    }).toMap
  }

  def getContainerContext(yarnConf: YarnConfiguration, command:String): ContainerLaunchContext = {    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
    ctx.setCommands(Seq(command)) 
    ctx.setEnvironment(getAppEnv(yarnConf))
    ctx.setLocalResources(getAMLocalResourcesMap(yarnConf))
    ctx.setTokens(getToken)
    ctx
  }

  def getToken():ByteBuffer = {
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData)
  }
  
  private def logEnvironmentVars(environment: Map[String, String]) {
    environment.foreach(pair => {
    val (key, value) = pair
    LOG.info(s"getAppEnv key=$key value=$value")
  })
 
  }
}