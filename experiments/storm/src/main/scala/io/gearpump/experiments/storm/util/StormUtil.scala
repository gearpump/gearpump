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

package io.gearpump.experiments.storm.util

import java.io.{File, FileOutputStream, IOException}
import java.util.jar.JarFile
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.generated._
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
import backtype.storm.utils.{ThriftTopologyUtils, Utils}
import clojure.lang.Atom
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.partitioner._
import io.gearpump.partitioner.{BroadcastPartitioner, Partitioner}
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.LogUtil
import org.apache.commons.io.{FileUtils, IOUtils}
import org.json.simple.JSONValue

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object StormUtil {
  val TOPOLOGY = "topology"
  val TASK_TO_COMPONENT = "task_to_component"
  val PROCESSOR_TO_COMPONENT = "processor_to_component"
  val STORM_CONFIG = "storm_config"
  val STORM_CONTEXT = "storm_context"
  val STORM_TASK_ID = 0

  private val LOG = LogUtil.getLogger(StormUtil.getClass)

  private val stormConfig = Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]]

  def getStormConfig(config: UserConfig, common: ComponentCommon)(implicit system: ActorSystem) : JMap[_, _] = {
    config.getString(STORM_CONFIG).foreach(conf => stormConfig.putAll(parseJsonStringToMap(conf)))
    Option(common.get_json_conf()).foreach(conf => stormConfig.putAll(parseJsonStringToMap(conf)))
    stormConfig
  }

  def parseJsonStringToMap(json: String): JMap[AnyRef, AnyRef] = {
    JSONValue.parse(json).asInstanceOf[JMap[AnyRef, AnyRef]]
  }

  def getComponentToStreamFields(topology: StormTopology): JMap[String, JMap[String, Fields]] = {
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    (spouts.map{ case (id, component) =>
      id -> getComponentToFields(component.get_common())
    } ++
        bolts.map { case (id, component) =>
          id -> getComponentToFields(component.get_common())
        }).toMap.asJava
  }

  def getComponentToFields(common: ComponentCommon): JMap[String, Fields] = {
    common.get_streams.map { case (sid, stream) =>
      sid -> new Fields(stream.get_output_fields())
    }.toMap.asJava
  }

  def getStormTaskId(componentId: String, componentToSortedTasks: JMap[String, JList[Integer]], taskId: TaskId): Integer = {
    componentToSortedTasks.get(componentId).get(taskId.index)
  }

  /**
   * target streams and components
   */
  def getTargets(componentId: String, topology: StormTopology): Map[String, Map[String, Grouping]] = {
    val componentIds = ThriftTopologyUtils.getComponentIds(topology)
    componentIds.flatMap { otherComponentId =>
      getInputs(otherComponentId, topology).toList.map(otherComponentId -> _)
    }.foldLeft(Map.empty[String, Map[String, Grouping]]) {
      (allTargets, componentAndInput) =>
        val (otherComponentId, (globalStreamId, grouping)) = componentAndInput
        val inputStreamId = globalStreamId.get_streamId()
        val inputComponentId = globalStreamId.get_componentId
        if (inputComponentId.equals(componentId)) {
          val curr = allTargets.getOrElse(inputStreamId, Map.empty[String, Grouping])
          allTargets + (inputStreamId -> (curr + (otherComponentId -> grouping)))
        } else {
          allTargets
        }
    }
  }

  def getComponentParallelism(componentId: String, topology: StormTopology): Int = {
    getParallelism(ThriftTopologyUtils.getComponentCommon(topology, componentId))
  }


  def getComponentToSortedTasks(taskToComponent: Map[Integer, String]): JMap[String, JList[Integer]] = {
    taskToComponent.groupBy(_._2).map { case (component, map) =>
      component -> map.toList.map(_._1).sorted.asJava }.asJava
  }

  def getInputs(componentId: String, topology: StormTopology): JMap[GlobalStreamId, Grouping] = {
    ThriftTopologyUtils.getComponentCommon(topology, componentId).get_inputs
  }

  def getGrouper(outFields: Fields, grouping: Grouping, numTasks: Int): Grouper = {
    grouping.getSetField match {
      case Grouping._Fields.FIELDS =>
        if (isGlobalGrouping(grouping)) {
          new GlobalGrouper
        } else {
          new FieldsGrouper(outFields, new Fields(grouping.get_fields()), numTasks)
        }
      case Grouping._Fields.SHUFFLE =>
        new ShuffleGrouper(numTasks)
      case Grouping._Fields.NONE =>
        new NoneGrouper(numTasks)
      case Grouping._Fields.CUSTOM_SERIALIZED =>
        throw new Exception("custom serialized grouping not supported")
      case Grouping._Fields.CUSTOM_OBJECT =>
        throw new Exception("custom object grouping not supported")
      case Grouping._Fields.LOCAL_OR_SHUFFLE =>
        // GearPump has built-in support for sending messages to local actor
        new ShuffleGrouper(numTasks)
    }
  }

  def groupingToPartitioner(outFields: Fields, grouping: Grouping, target: String): Partitioner = {
    grouping.getSetField match {
      case Grouping._Fields.ALL =>
        new BroadcastPartitioner
      case Grouping._Fields.CUSTOM_SERIALIZED =>
        throw new Exception("custom serialized grouping not supported")
      case Grouping._Fields.CUSTOM_OBJECT =>
        throw new Exception("custom object grouping not supported")
      case _ =>
        new StormPartitioner(target)
    }
  }

  def isGlobalGrouping(grouping: Grouping): Boolean = {
    grouping.getSetField == Grouping._Fields.FIELDS &&
        grouping.get_fields.isEmpty
  }

  def getParallelism(component: ComponentCommon): Int = {
    val parallelism = component.get_parallelism_hint()
    if (parallelism == 0) {
      // for global grouping
      1
    } else {
      parallelism
    }
  }

    // a workaround to support storm ShellBolt
    def mkPidDir: String = {
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
    def mkCodeDir: String = {
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

  def buildTopologyContext(topology: StormTopology, stormConf: JMap[_, _],
      taskToComponent: JMap[Integer, String], componentToSortedTasks: JMap[String, JList[Integer]],
      componentToStreamFields: JMap[String, JMap[String, Fields]],
      componentId: String, stormTaskId: Int, multiLang: Boolean = false): TopologyContext = {
    var codeDir: String = null
    var pidDir: String = null
    if (multiLang) {
      codeDir = mkCodeDir
      pidDir = mkPidDir
    }
    new TopologyContext(topology, stormConf, taskToComponent, componentToSortedTasks,
      componentToStreamFields, null, codeDir, pidDir, stormTaskId, null, null, null, null, null,
      new JHashMap[AnyRef, AnyRef], new Atom(false))  }
}
