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
import backtype.storm.Config
import backtype.storm.generated.{Bolt, ComponentCommon, SpoutSpec, StormTopology}
import backtype.storm.spout.{ISpout, SpoutOutputCollector}
import backtype.storm.task.{GeneralTopologyContext, IBolt, OutputCollector, TopologyContext}
import backtype.storm.tuple.{Fields, Tuple, TupleImpl}
import backtype.storm.utils.Utils
import clojure.lang.Atom
import io.gearpump.experiments.storm.processor.StormBoltOutputCollector
import io.gearpump.experiments.storm.producer.StormSpoutOutputCollector
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.experiments.storm.util.{StormOutputCollector, StormUtil}
import io.gearpump.streaming.DAG
import io.gearpump.streaming.task._
import io.gearpump.util.{Constants, LogUtil}
import io.gearpump.{Message, TimeStamp}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

/**
 * subclass wraps Storm Spout and Bolt, and their lifecycles
 * hides the complexity from Gearpump applications
 */
trait GearpumpStormComponent {
  /**
   * invoked at Task.onStart
   * @param startTime task start time
   */
  def start(startTime: StartTime): Unit

  /**
   * invoked at Task.onNext
   * @param message incoming message
   */
  def next(message: Message): Unit

  /**
   * invoked at Task.onStop
   */
  def stop: Unit = {}
}

object GearpumpStormComponent {
  private val LOG: Logger = LogUtil.getLogger(classOf[GearpumpStormComponent])

  object GearpumpSpout {
    def apply(topology: StormTopology, config: JMap[AnyRef, AnyRef],
             spoutSpec: SpoutSpec, taskContext: TaskContext): GearpumpSpout = {
      val componentCommon = spoutSpec.get_common()
      val normalizedConfig = normalizeConfig(config.toMap, componentCommon)
      val getTopologyContext = (dag: DAG, taskId: TaskId) => {
        val stormTaskId = gearpumpTaskIdToStorm(taskId)
        buildTopologyContext(dag, topology, normalizedConfig, stormTaskId)
      }
      val spout = Utils.getSetComponentObject(spoutSpec.get_spout_object()).asInstanceOf[ISpout]
      val ackEnabled = StormUtil.ackEnabled(config)
      if (ackEnabled) {
        val className = spout.getClass.getName
        if (!isSequentiallyReplayableSpout(className)) {
          LOG.warn(s"at least once is not supported for $className")
        }
      }
      val getOutputCollector = (taskContext: TaskContext, topologyContext: TopologyContext) => {
        new StormSpoutOutputCollector(
          StormOutputCollector(taskContext, topologyContext), spout, ackEnabled)
      }
      GearpumpSpout(
        normalizedConfig,
        spout,
        askAppMasterForDAG,
        getTopologyContext,
        getOutputCollector,
        ackEnabled,
        taskContext)
    }

    private def isSequentiallyReplayableSpout(className: String): Boolean = {
      className.equals("storm.kafka.KafkaSpout")
    }
  }

  case class GearpumpSpout(
      config: JMap[AnyRef, AnyRef],
      spout: ISpout,
      getDAG: ActorRef => DAG,
      getTopologyContext: (DAG, TaskId) => TopologyContext,
      getOutputCollector: (TaskContext, TopologyContext) => StormSpoutOutputCollector,
      ackEnabled: Boolean,
      taskContext: TaskContext)
    extends GearpumpStormComponent {

    private var collector: StormSpoutOutputCollector = null


    override def start(startTime: StartTime): Unit = {
      import taskContext.{appMaster, taskId}

      val dag = getDAG(appMaster)
      val topologyContext = getTopologyContext(dag, taskId)
      collector = getOutputCollector(taskContext, topologyContext)
      spout.open(config, topologyContext, new SpoutOutputCollector(collector))
    }

    override def next(message: Message): Unit = {
      spout.nextTuple()
    }

    /**
     * @return timeout in milliseconds if enabled
     */
    def getMessageTimeout: Option[Long] = {
      StormUtil.getBoolean(config, Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS).flatMap {
        timeoutEnabled =>
          if (timeoutEnabled) {
            StormUtil.getInt(config, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).map(_ * 1000L)
          } else {
            None
          }
      }
    }

    def checkpoint(clock: TimeStamp): Unit = {
      collector.ackPendingMessage(clock)
    }

    def timeout(timeoutMillis: Long): Unit = {
      collector.failPendingMessage(timeoutMillis)
    }
  }

  object GearpumpBolt {
    def apply(topology: StormTopology, config: JMap[AnyRef, AnyRef],
              boltSpec: Bolt, taskContext: TaskContext): GearpumpBolt = {
      val normalizedConfig = normalizeConfig(config.toMap, boltSpec.get_common())
      val getTopologyContext = (dag: DAG, taskId: TaskId) => {
        val stormTaskId = gearpumpTaskIdToStorm(taskId)
        buildTopologyContext(dag, topology, normalizedConfig, stormTaskId)
      }
      val getGeneralTopologyContext = (dag: DAG) => {
        buildGeneralTopologyContext(dag, topology, normalizedConfig)
      }
      val getOutputCollector = (taskContext: TaskContext, topologyContext: TopologyContext) => {
        StormOutputCollector(taskContext, topologyContext)
      }
      val getTickTuple = (topologyContext: GeneralTopologyContext, freq: Int) => {
        new TupleImpl(topologyContext, List(freq.asInstanceOf[java.lang.Integer]),
          SYSTEM_TASK_ID, SYSTEM_TICK_STREAM_ID, null)
      }
      GearpumpBolt(
        normalizedConfig,
        Utils.getSetComponentObject(boltSpec.get_bolt_object()).asInstanceOf[IBolt],
        askAppMasterForDAG,
        getTopologyContext,
        getGeneralTopologyContext,
        getOutputCollector,
        getTickTuple,
        taskContext)
    }
  }

  case class GearpumpBolt(
      config: JMap[AnyRef, AnyRef],
      bolt: IBolt,
      getDAG: ActorRef => DAG,
      getTopologyContext: (DAG, TaskId) => TopologyContext,
      getGeneralTopologyContext: DAG => GeneralTopologyContext,
      getOutputCollector: (TaskContext, TopologyContext) => StormOutputCollector,
      getTickTuple: (GeneralTopologyContext, Int) => Tuple,
      taskContext: TaskContext)
    extends GearpumpStormComponent {
    import taskContext.{appMaster, taskId}

    private var collector: StormOutputCollector = null
    private var topologyContext: TopologyContext = null
    private var generalTopologyContext: GeneralTopologyContext = null
    private var tickTuple: Tuple = null

    override def start(startTime: StartTime): Unit = {
      val dag = getDAG(appMaster)
      topologyContext = getTopologyContext(dag, taskId)
      generalTopologyContext = getGeneralTopologyContext(dag)
      collector = getOutputCollector(taskContext, topologyContext)
      val delegate = new StormBoltOutputCollector(collector)
      bolt.prepare(config, topologyContext, new OutputCollector(delegate))
    }

    override def next(message: Message): Unit = {
      val timestamp = message.timestamp
      collector.setTimestamp(timestamp)
      bolt.execute(message.msg.asInstanceOf[GearpumpTuple].toTuple(generalTopologyContext, timestamp))
    }

    def getTickFrequency: Option[Int] = {
      StormUtil.getInt(config, Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)
    }

    /**
     * invoked at TICK message when "topology.tick.tuple.freq.secs" is configured
     * @param freq tick frequency
     */
    def tick(freq: Int): Unit = {
      if (null == tickTuple) {
        tickTuple = getTickTuple(generalTopologyContext, freq)
      }
      bolt.execute(tickTuple)
    }
  }

  /**
   * normalize general config with per component configs
   * "topology.transactional.id" and "topology.tick.tuple.freq.secs"
   * @param stormConfig general config for all components
   * @param componentCommon common component parts
   */
  private def normalizeConfig(stormConfig: Map[AnyRef, AnyRef],
                              componentCommon: ComponentCommon): JMap[AnyRef, AnyRef] = {
    val config: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    config.putAll(stormConfig)
    val componentConfig = parseJsonStringToMap(componentCommon.get_json_conf())
    Option(componentConfig.get(Config.TOPOLOGY_TRANSACTIONAL_ID))
      .foreach(config.put(Config.TOPOLOGY_TRANSACTIONAL_ID, _))
    Option(componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS))
      .foreach(config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, _))
    config
  }

  private def askAppMasterForDAG(appMaster: ActorRef): DAG = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val dagFuture = (appMaster ? GetDAG).asInstanceOf[Future[DAG]]
    Await.result(dagFuture, timeout.duration)
  }

  private def buildGeneralTopologyContext(dag: DAG, topology: StormTopology, stormConf: JMap[_, _]): GeneralTopologyContext = {
    val taskToComponent = getTaskToComponent(dag)
    val componentToSortedTasks: JMap[String, JList[Integer]] = getComponentToSortedTasks(taskToComponent)
    val componentToStreamFields: JMap[String, JMap[String, Fields]] = getComponentToStreamFields(topology)
    new GeneralTopologyContext(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamFields, null)
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

  private def getComponentToStreamFields(topology: StormTopology): JMap[String, JMap[String, Fields]] = {
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

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
    taskToComponent
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
}