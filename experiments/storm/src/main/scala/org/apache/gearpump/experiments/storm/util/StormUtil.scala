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

package org.apache.gearpump.experiments.storm.util

import java.io.{File, FileOutputStream, IOException}
import java.util.jar.JarFile
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.generated.{ComponentCommon, StormTopology}
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.{TupleImpl, Tuple, Fields}
import backtype.storm.utils.Utils
import clojure.lang.Atom
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming._
import org.apache.gearpump.util.LogUtil
import org.json.simple.JSONValue

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object StormUtil {
  val TOPOLOGY = "topology"
  val PROCESSOR_TO_COMPONENT = "processor_to_component"
  val STORM_CONFIG = "storm_config"
  val STORM_CONTEXT = "storm_context"

  private val LOG = LogUtil.getLogger(StormUtil.getClass)

  private val stormConfig = Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]]



  def getTopology(config: UserConfig)(implicit system: ActorSystem) : StormTopology = {
    config.getValue[StormTopology](TOPOLOGY)
      .getOrElse(throw new RuntimeException("storm topology is not found"))
  }

  def getStormConfig(config: UserConfig)(implicit system: ActorSystem) : JMap[_, _] = {
    val serConf = config.getValue[String](STORM_CONFIG)
      .getOrElse(throw new RuntimeException("storm config not found"))
    val conf = JSONValue.parse(serConf).asInstanceOf[JMap[AnyRef, AnyRef]]
    stormConfig.putAll(conf)
    stormConfig
  }

  def getTopologyContext(topology: StormTopology,
                         stormConf: JMap[_, _],
                         processorToComponent: Map[Int, String],
                         taskId: Int,
                         multiLang: Boolean): TopologyContext = {
    if (multiLang) {
      getTopologyContext(topology, stormConf, processorToComponent, taskId, mkCodeDir, mkPidDir)
    } else {
      getTopologyContext(topology, stormConf, processorToComponent, taskId)
    }
  }

  def getTopologyContext(topology: StormTopology,
                         stormConf: JMap[_, _],
                         processorToComponent: Map[Int, String],
                         taskId: Int,
                         codeDir: => String = null,
                         pidDir: => String = null): TopologyContext = {
    val taskToComponent = getTaskToComponent(processorToComponent)
    val componentToSortedTasks = getComponentToSortedTasks(processorToComponent)
    val componentToStreamFields = getComponentToStreamFields(topology)
    new TopologyContext(
      topology, stormConf, taskToComponent,
      componentToSortedTasks, componentToStreamFields, null,
      codeDir, pidDir, taskId, null, null, null, null, null, new JHashMap[AnyRef, AnyRef], new Atom(false))
  }

  // a workaround to support storm ShellBolt
  private def mkPidDir: String = {
    val pidDir = FileUtils.getTempDirectoryPath + File.separator + "pid"
    try {
      FileUtils.forceMkdir(new File(pidDir))
    } catch {
      case ex: IOException =>
        LOG.error(s"failed to create pid directory $pidDir")
    }
    pidDir
  }

  // a workaround to support storm ShellBolt
  private def mkCodeDir: String = {
    val jarPath = System.getProperty("java.class.path").split(":").last
    val destDir = FileUtils.getTempDirectoryPath + File.separator + "storm"

    try {
      FileUtils.forceMkdir(new File(destDir))

      val jar = new JarFile(jarPath)
      val enumEntries = jar.entries()
      enumEntries.foreach { entry =>
        val file = new File(destDir + File.separator + entry.getName)
        if (!entry.isDirectory) {
          file.getParentFile.mkdirs()

          val is = jar.getInputStream(entry)
          val fos = new FileOutputStream(file)
          try {
            IOUtils.copy(is, fos)
          } catch {
            case ex: IOException =>
              LOG.error(s"failed to copy data from ${entry.getName} to ${file.getName}")
          } finally {
            fos.close()
            is.close()
          }
        }
      }
    } catch {
      case ex: IOException =>
        LOG.error(s"could not extract $destDir from $jarPath")
    }

    destDir + File.separator + "resources"
  }

  def getTaskToComponent(processorToComponent: Map[Int, String]): JMap[Integer, String] = {
    processorToComponent.map { case (pid, cid) =>
      (pid.asInstanceOf[Integer], cid)
    }.asJava
  }

  def getComponentToSortedTasks(processorToComponent: Map[Int, String]): JMap[String, JList[Integer]] = {
    processorToComponent.map { case (pid, cid) =>
      (cid, List(pid.asInstanceOf[Integer]).asJava)
    }.asJava
  }

  def getComponentToStreamFields(topology: StormTopology) = {
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    def getComponentToFields(common: ComponentCommon) = {
         common.get_streams.map { case (sid, stream) =>
          sid -> new Fields(stream.get_output_fields())
        }.toMap
    }
    (spouts.map{ case (id, component) =>
      id -> getComponentToFields(component.get_common()).asJava
    } ++
      bolts.map { case (id, component) =>
        id -> getComponentToFields(component.get_common()).asJava
      }).toMap.asJava
  }
}
