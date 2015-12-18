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

package io.gearpump.experiments.yarn.client

import java.io.{OutputStream, ByteArrayOutputStream, BufferedOutputStream, FileOutputStream, InputStream, ByteArrayInputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{ActiveConfig, GetActiveConfig}
import io.gearpump.experiments.yarn.glue.{FileSystem, YarnClient, YarnConfig}
import io.gearpump.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import java.util.Random
import scala.concurrent.Await
import scala.util.Try
import io.gearpump.experiments.yarn.glue.Records._
class LaunchClusterSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var system: ActorSystem = null

  val rand = new Random()

  private def randomArray(size: Int): Array[Byte] = {
    val array = new Array[Byte](size)
    rand.nextBytes(array)
    array
  }
  val appId = ApplicationId.newInstance(0L, 0)

  val akka = ConfigFactory.parseString(

    """
      |gearpump {
      |  yarn {
      |    client {
      |      package -path = "/user/gearpump/gearpump.zip"
      |    }
      |
      |    applicationmaster {
      |      ## Memory of YarnAppMaster
      |        command = "$JAVA_HOME/bin/java -Xmx512m"
      |      memory = "512"
      |      vcores = "1"
      |      queue = "default"
      |    }
      |
      |    master {
      |      ## Memory of master daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      containers = "2"
      |      memory = "512"
      |      vcores = "1"
      |    }
      |
      |    worker {
      |      ## memory of worker daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      containers = "4"
      |      ## This also contains all memory for child executors.
      |      memory = "4096"
      |      vcores = "1"
      |    }
      |    services {
      |      enabled = true
      |    }
      |  }
      |}
    """.stripMargin).withFallback(TestUtil.DEFAULT_CONFIG)


  override def beforeAll() = {
    system = ActorSystem(getClass.getSimpleName, akka)
  }

  override def afterAll() = {
    system.shutdown()
    system.awaitTermination()
  }

  it should "reject non-zip files" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])

    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver)
    val packagePath = "gearpump.zip2"
    assert(Try(launcher.submit("gearpump", packagePath)).isFailure)
  }


  it should "reject if we cannot find the package file on HDFS" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])

    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver)
    val packagePath = "gearpump.zip"
    when(fs.exists(anyString)).thenReturn(false)
    assert(Try(launcher.submit("gearpump", packagePath)).isFailure)
  }

  it should "throw when package exists on HDFS, but the file is corrupted" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])

    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver)
    val packagePath = "gearpump.zip"
    when(fs.exists(anyString)).thenReturn(true)

    val content = new ByteArrayInputStream(randomArray(10))
    when(fs.open(anyString)).thenReturn(content)
    assert(Try(launcher.submit("gearpump", packagePath)).isFailure)
    content.close()
  }

  it should "throw when the HDFS package version is not consistent with local version" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])

    val version = "gearpump-0.2"
    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver, version)
    val packagePath = "gearpump.zip"
    when(fs.exists(anyString)).thenReturn(true)

    val oldVesion = "gearpump-0.1"
    when(fs.open(anyString)).thenReturn(zipInputStream(oldVesion))
    assert(Try(launcher.submit("gearpump", packagePath)).isFailure)
  }

  it should "upload config file to HDFS when submitting" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])

    val version = "gearpump-0.2"
    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver, version)
    val packagePath = "gearpump.zip"

    val out = mock(classOf[OutputStream])
    when(fs.exists(anyString)).thenReturn(true)
    when(fs.create(anyString)).thenReturn(out)
    when(fs.getHomeDirectory).thenReturn("/root")

    when(fs.open(anyString)).thenReturn(zipInputStream(version))

    val report = mock(classOf[ApplicationReport])
    when(yarnClient.awaitApplication(any[ApplicationId], anyLong())).thenReturn(report)

    when(report.getApplicationId).thenReturn(appId)
    when(yarnClient.createApplication).thenReturn(appId)
    assert(appId == launcher.submit("gearpump", packagePath))

    // 3 config files are uploaded to HDFS, one is akka.conf, one is yarn-site.xml, one is log4j.properties.
    verify(fs, times(3)).create(anyString)
    verify(out, times(3)).close()

    //val workerResources = ArgumentCaptor.forClass(classOf[List[Resource]])
    val expectedCommand = "$JAVA_HOME/bin/java -Xmx512m -cp conf:pack/gearpump-0.2/conf:pack/gearpump-0.2/dashboard:pack/gearpump-0.2/lib/*:pack/gearpump-0.2/lib/daemon/*:pack/gearpump-0.2/lib/services/*:pack/gearpump-0.2/lib/yarn/*:$CLASSPATH -Dgearpump.home={{LOCAL_DIRS}}/{{CONTAINER_ID}}/pack/gearpump-0.2 -Dgearpump.binary-version-with-scala-version=gearpump-0.2 -Dgearpump.log.daemon.dir=<LOG_DIR> -Dgearpump.log.application.dir=<LOG_DIR>  io.gearpump.experiments.yarn.appmaster.YarnAppMaster  -conf /root/.gearpump_app0/conf/ -package gearpump.zip 2>&1 | /usr/bin/tee -a <LOG_DIR>/stderr"
    verify(yarnClient).submit("gearpump", appId, expectedCommand,
      Resource.newInstance(512, 1), "default",
      "gearpump.zip", "/root/.gearpump_app0/conf/")
  }

  it should "save active config from Gearpump cluster" in {
    val yarnConfig = mock(classOf[YarnConfig])
    val yarnClient = mock(classOf[YarnClient])
    val fs = mock(classOf[FileSystem])
    val appMasterResolver = mock(classOf[AppMasterResolver])
    val appMaster = TestProbe()

    val version = "gearpump-0.2"
    val launcher = new LaunchCluster(akka, yarnConfig, yarnClient, fs, system, appMasterResolver, version)
    val outputPath = java.io.File.createTempFile("LaunchClusterSpec", ".conf")

    when(appMasterResolver.resolve(any[ApplicationId], anyInt)).thenReturn(appMaster.ref)
    val fileFuture = launcher.saveConfig(appId, outputPath.getPath)
    appMaster.expectMsgType[GetActiveConfig]
    appMaster.reply(ActiveConfig(ConfigFactory.empty()))

    import scala.concurrent.duration._
    val file = Await.result(fileFuture, 30 seconds).asInstanceOf[java.io.File]

    assert(!FileUtils.read(file).isEmpty)
    file.delete()
  }

  private def zipInputStream(version: String): InputStream = {
    val bytes = new ByteArrayOutputStream(1000)
    val zipOut = new ZipOutputStream(bytes)

    // not available on BufferedOutputStream
    zipOut.putNextEntry(new ZipEntry(s"$version/README.md"))
    zipOut.write("README".getBytes())
    // not available on BufferedOutputStream
    zipOut.closeEntry()
    zipOut.close()
    new ByteArrayInputStream(bytes.toByteArray)
  }
}