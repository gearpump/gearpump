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

package io.gearpump.experiments.storm.topology

import java.io.{File, FileOutputStream, IOException}
import java.util.jar.JarFile
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ActorRef
import akka.pattern.ask
import backtype.storm.generated.{Bolt, ComponentCommon, SpoutSpec, StormTopology}
import backtype.storm.spout.{ISpout, SpoutOutputCollector}
import backtype.storm.task.{IBolt, OutputCollector, TopologyContext}
import backtype.storm.tuple.{Fields, TupleImpl}
import backtype.storm.utils.Utils
import clojure.lang.Atom
import io.gearpump.Message
import io.gearpump.experiments.storm.processor.StormBoltOutputCollector
import io.gearpump.experiments.storm.producer.StormSpoutOutputCollector
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormOutputCollector
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.DAG
import io.gearpump.streaming.appmaster.AppMaster.GetDAG
import io.gearpump.streaming.task.{StartTime, TaskContext, TaskId}
import io.gearpump.util.{Constants, LogUtil}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

trait GearpumpStormComponent {
  def start(startTime: StartTime): Unit

  def next(message: Message): Unit

  def stop: Unit = {}
}

object GearpumpStormComponent {
  private val LOG: Logger = LogUtil.getLogger(classOf[GearpumpStormComponent])

  case class GearpumpSpout(topology: StormTopology, spoutId: String, spoutSpec: SpoutSpec, taskContext: TaskContext)
      extends GearpumpStormComponent {
    private val spout: ISpout = Utils.getSetComponentObject(spoutSpec.get_spout_object()).asInstanceOf[ISpout]
    private val config: JMap[AnyRef, AnyRef] =
      parseJsonStringToMap(spoutSpec.get_common().get_json_conf())
    private var collector: StormOutputCollector = null

    override def start(startTime: StartTime): Unit = {
      import taskContext.{appMaster, taskId}

      val dag = askAppMasterForDAG(appMaster)
      val stormTaskId = gearpumpTaskIdToStorm(taskId)
      val topologyContext = buildTopologyContext(dag, topology, config, stormTaskId)
      collector = new StormOutputCollector(taskContext, topologyContext)
      val delegate = new StormSpoutOutputCollector(collector)
      spout.open(config, topologyContext, new SpoutOutputCollector(delegate))
    }

    override def next(message: Message): Unit = {
      collector.setTimestamp(System.currentTimeMillis())
      spout.nextTuple()

    }
  }

  case class GearpumpBolt(topology: StormTopology, boltId: String, boltSpec: Bolt, taskContext: TaskContext)
      extends GearpumpStormComponent {
    import taskContext.{appMaster, taskId}

    private val bolt: IBolt = Utils.getSetComponentObject(boltSpec.get_bolt_object()).asInstanceOf[IBolt]
    private val config: JMap[AnyRef, AnyRef] =
      parseJsonStringToMap(boltSpec.get_common().get_json_conf())
    private var collector: StormOutputCollector = null
    private var topologyContext: TopologyContext = null
    private var tickTuple: TupleImpl = null


    override def start(startTime: StartTime): Unit = {
      val dag = askAppMasterForDAG(appMaster)
      val stormTaskId = gearpumpTaskIdToStorm(taskId)
      topologyContext = buildTopologyContext(dag, topology, config, stormTaskId)
      collector = new StormOutputCollector(taskContext, topologyContext)
      val delegate = new StormBoltOutputCollector(collector)
      bolt.prepare(config, topologyContext, new OutputCollector(delegate))
    }

    override def next(message: Message): Unit = {
      collector.setTimestamp(message.timestamp)
      bolt.execute(message.msg.asInstanceOf[GearpumpTuple].toTuple(topologyContext))
    }

    def getTickFrequency: Option[Long] = {
      Option(config.get(TICK_TUPLE_FREQ_SECS)).asInstanceOf[Option[Long]]
    }

    def tick(freq: Long): Unit = {
      if (null == tickTuple) {
        tickTuple = new TupleImpl(topologyContext, List(freq.asInstanceOf[java.lang.Long]),
          SYSTEM_TASK_ID, SYSTEM_TICK_STREAM_ID, null)
      }
      bolt.execute(tickTuple)
    }
  }



  private def getComponentToStreamFields(topology: StormTopology): JMap[String, JMap[String, Fields]] = {
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    val systemStreamFields = new JHashMap[String, Fields]
    systemStreamFields.put(SYSTEM_TICK_STREAM_ID, new Fields(SYSTEM_COMPONENT_OUTPUT_FIELDS))

    (spouts.map { case (id, component) => id -> getComponentToFields(component.get_common()) } ++
        bolts.map { case (id, component) => id -> getComponentToFields(component.get_common())} ++
        Map(SYSTEM_COMPONENT_ID -> Map(SYSTEM_TICK_STREAM_ID -> new Fields(SYSTEM_COMPONENT_OUTPUT_FIELDS)).asJava)
        ).toMap.asJava
  }

  private def getComponentToFields(common: ComponentCommon): JMap[String, Fields] = {
    common.get_streams.map { case (sid, stream) =>
      sid -> new Fields(stream.get_output_fields())
    }.toMap.asJava
  }

  private def getComponentToSortedTasks(taskToComponent: Map[Integer, String]): JMap[String, JList[Integer]] = {
    taskToComponent.groupBy(_._2).map { case (component, map) =>
      val sortedTasks = map.keys.toList.sorted.asJava
      component -> sortedTasks
    }.asJava
  }

  private def getTaskToComponent(dag: DAG): Map[Integer, String] = {
    val taskToComponent = dag.processors.flatMap { case (processorId, processorDescription) =>
      val parallelism = processorDescription.parallelism
      val component = processorDescription.taskConf.getString(STORM_COMPONENT).get
      (0 until parallelism).map(index => gearpumpTaskIdToStorm(TaskId(processorId, index)) -> component)
    }
    taskToComponent + (SYSTEM_TASK_ID -> SYSTEM_COMPONENT_ID)
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

  private def askAppMasterForDAG(appMaster: ActorRef): DAG = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val dagFuture = (appMaster ? GetDAG).asInstanceOf[Future[DAG]]
    Await.result(dagFuture, timeout.duration)
  }

  private def buildTopologyContext(dag: DAG, topology: StormTopology, stormConf: JMap[_, _], stormTaskId: Integer): TopologyContext = {
    val taskToComponent = getTaskToComponent(dag)
    val componentToSortedTasks: JMap[String, JList[Integer]] = getComponentToSortedTasks(taskToComponent)
    val componentToStreamFields: JMap[String, JMap[String, Fields]] = getComponentToStreamFields(topology)
    val codeDir = mkCodeDir
    val pidDir = mkPidDir

    new TopologyContext(topology, stormConf, taskToComponent, componentToSortedTasks,
      componentToStreamFields, null, codeDir, pidDir, stormTaskId, null, null, null, null, new JHashMap[String, AnyRef],
      new JHashMap[AnyRef, AnyRef], new Atom(false))
  }
}