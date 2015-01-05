/*
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

package org.apache.gearpump.util

import java.io.File
import java.net.ServerSocket

import com.typesafe.config.Config
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.transport.HostPort

import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.sys.process.Process
<<<<<<< HEAD
import scala.util.Try
=======
>>>>>>> 56c1841... add more UT

object Util {
  val LOG = LogUtil.getLogger(getClass)

  def getCurrentClassPath : Array[String] = {
    val classpath = System.getProperty("java.class.path");
    val classpathList = classpath.split(File.pathSeparator);
    classpathList
  }

<<<<<<< HEAD
  def startProcess(options : Array[String], classPath : Array[String], mainClass : String,
                   arguments : Array[String]) : Process = {
=======
  def startProcess(options: Array[String], classPath: Array[String], mainClass: String,
                   arguments: Array[String]): Process = {
>>>>>>> 56c1841... add more UT
    val java = System.getProperty("java.home") + "/bin/java"
    val command = List(java) ++ options ++ List("-cp", classPath.mkString(File.pathSeparator), mainClass) ++ arguments
    LOG.info(s"Starting executor process $command...")
    val process = Process(command).run(new ProcessLogRedirector())
    process
  }

  /**
   * hostList format: host1:port1,host2:port2,host3:port3...
   */
  def parseHostList(hostList : String) : List[HostPort] = {
    val masters = hostList.trim.split(",").map { address =>
      val hostAndPort = address.split(":")
      HostPort(hostAndPort(0), hostAndPort(1).toInt)
    }
    masters.toList
  }

  def randInt: Int = {
    ThreadLocalRandom.current.nextInt()
  }

  def findFreePort: Try[Int] = {
    Try {
      val socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      val port = socket.getLocalPort();
      socket.close;
      port
    }
  }

  case class JvmSetting(vmargs : Array[String], classPath : Array[String])

  case class AppJvmSettings(appMater : JvmSetting, executor : JvmSetting)

  def resolveJvmSetting(user : UserConfig, sys : Config) : AppJvmSettings = {

    import Constants._

    val appMasterVMArgs = user.getString(GEARPUMP_APPMASTER_ARGS)
      .getOrElse(sys.getString(GEARPUMP_APPMASTER_ARGS)).split(" ")
    val executorVMArgs = user.getString(GEARPUMP_EXECUTOR_ARGS)
      .getOrElse(sys.getString(GEARPUMP_EXECUTOR_ARGS)).split(" ")

    var appMasterClass = user.getString(GEARPUMP_APPMASTER_EXTRA_CLASSPATH).map {
        _.split(File.pathSeparator)
      }.getOrElse(Array.empty[String])

    appMasterClass ++= sys.getString(GEARPUMP_APPMASTER_EXTRA_CLASSPATH).split(File.pathSeparator).asInstanceOf[Array[String]]

    var executorClass = user.getString(GEARPUMP_EXECUTOR_EXTRA_CLASSPATH).map {
      _.split(File.pathSeparator)
    }.getOrElse(Array.empty[String])

    executorClass ++= sys.getString(GEARPUMP_EXECUTOR_EXTRA_CLASSPATH).split(File.pathSeparator).asInstanceOf[Array[String]]

    AppJvmSettings(JvmSetting(appMasterVMArgs, appMasterClass ), JvmSetting(executorVMArgs, executorClass))
  }
}
