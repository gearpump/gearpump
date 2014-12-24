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

package org.apache.gearpump.cluster.client

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import org.apache.gearpump.cluster._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{LogUtil, Configs, Util}
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ListBuffer

//TODO: add interface to query master here
class ClientContext(masters: Iterable[HostPort]) {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  val system = ActorSystem(s"client${Util.randInt()}" , Configs.loadApplicationConfig())

  val master = system.actorOf(Props(classOf[MasterProxy], masters), MASTER)

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gear.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app : Application) : Int = {
    submit(app, System.getProperty(GEAR_APP_JAR))
  }

  def submit(app : Application, jarPath: String) : Int = {
    val client = new MasterClient(master)
    if (jarPath == null) {
      client.submitApplication(app, None)
    } else {
      val appJar = loadFile(jarPath)
      client.submitApplication(app, Option(appJar))
    }
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  def cleanup() : Unit = {
    system.shutdown()
  }

  private def loadFile(jarPath : String) : AppJar = {
    val jarFile = new java.io.File(jarPath)
    val fis = new FileInputStream(jarFile)
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val buf = ListBuffer[Byte]()
    var b = fis.read()
    while (b != -1) {
      buf.append(b.byteValue)
      b = fis.read()
    }
    AppJar(jarFile.getName, buf.toArray)
  }
}

object ClientContext {
  /**
   * masterList is a list of master node address
   * host1:port,host2:port2,host3:port3
   */
  def apply(masterList : String) = {
    new ClientContext(Util.parseHostList(masterList))
  }

  def apply(masters: Iterable[HostPort]) = new ClientContext(masters)
}
