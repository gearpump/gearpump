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

package io.gearpump.experiments.storm.main

import java.io.{File, FileOutputStream, FileWriter}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.{Map => JMap, UUID}

import akka.actor.ActorSystem
import backtype.storm.Config
import backtype.storm.generated._
import backtype.storm.security.auth.{ThriftConnectionType, ThriftServer}
import backtype.storm.utils.TimeCacheMap.ExpiredCallback
import backtype.storm.utils.{TimeCacheMap, Utils}
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.{MasterToAppMaster, UserConfig}
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.experiments.storm.topology.GearpumpStormTopology
import io.gearpump.experiments.storm.util.{GraphBuilder, StormConstants, StormUtil}
import io.gearpump.streaming.StreamApplication
import io.gearpump.util.{FileUtils, AkkaApp, Constants, LogUtil}
import org.apache.storm.shade.org.json.simple.JSONValue
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor
import org.slf4j.Logger

import scala.collection.JavaConverters._

object GearpumpNimbus extends AkkaApp with ArgumentsParser {
  private val THRIFT_PORT = StormUtil.getThriftPort
  private val OUTPUT = "output"

  override val options: Array[(String, CLIOption[Any])] = Array(
    OUTPUT -> CLIOption[String]("<output path for configuration file>", required = false, defaultValue = Some("app.yaml"))
  )

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {
    val parsed = parse(args)
    val output = parsed.getString(OUTPUT)
    val akkaConf = updateClientConfig(inputAkkaConf)
    val system = ActorSystem("storm", akkaConf)
    val clientContext = new ClientContext(akkaConf, system, null)
    val stormConf = Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]]
    val thriftConf: JMap[String, String] = Map(
      Config.NIMBUS_HOST -> akkaConf.getString(Constants.GEARPUMP_HOSTNAME),
      Config.NIMBUS_THRIFT_PORT -> s"$THRIFT_PORT").asJava
    updateOutputStormConfig(thriftConf, output)
    stormConf.putAll(thriftConf)
    val thriftServer = createServer(clientContext, stormConf)
    thriftServer.serve()
    system.awaitTermination()
  }

  private def createServer(clientContext: ClientContext, stormConf: JMap[AnyRef, AnyRef]): ThriftServer = {
    val processor = new Nimbus.Processor[GearpumpNimbus](new GearpumpNimbus(clientContext, stormConf))
    val connectionType = ThriftConnectionType.NIMBUS
    new ThriftServer(stormConf, processor, connectionType)
  }

  private def updateOutputStormConfig(conf: JMap[String, String], output: String): Unit = {
    // read existing config
    val outputConfig = Utils.findAndReadConfigFile(output, false).asInstanceOf[JMap[AnyRef, AnyRef]]
    outputConfig.putAll(conf)
    val yaml = new Yaml(new SafeConstructor)
    val writer = new FileWriter(new File(output))
    yaml.dump(outputConfig, writer)
  }

  import Constants._
  private def updateClientConfig(config: Config): Config = {
    val storm = s"<${GEARPUMP_HOME}>/lib/storm/*"
    val appClassPath = s"$storm${File.pathSeparator}" + config.getString(GEARPUMP_APPMASTER_EXTRA_CLASSPATH)
    val executorClassPath = s"$storm${File.pathSeparator}" + config.getString(Constants.GEARPUMP_EXECUTOR_EXTRA_CLASSPATH)

    val updated = config.withValue(GEARPUMP_APPMASTER_EXTRA_CLASSPATH, ConfigValueFactory.fromAnyRef(appClassPath))
        .withValue(GEARPUMP_EXECUTOR_EXTRA_CLASSPATH, ConfigValueFactory.fromAnyRef(executorClassPath))

    if (config.hasPath(StormConstants.STORM_SERIALIZATION_FRAMEWORK)) {
      val serializerConfig = ConfigValueFactory.fromAnyRef(config.getString(StormConstants.STORM_SERIALIZATION_FRAMEWORK))
      updated.withValue(GEARPUMP_SERIALIZER_POOL, serializerConfig)
    } else {
      updated
    }
  }

}

class GearpumpNimbus(clientContext: ClientContext, stormConf: JMap[AnyRef, AnyRef]) extends Nimbus.Iface {
  private val LOG: Logger = LogUtil.getLogger(classOf[GearpumpNimbus])
  private var applications = Map.empty[String, Int]
  private var topologies = Map.empty[String, (StormTopology, JMap[AnyRef, AnyRef])]
  private val expireSeconds = StormUtil.getInt(stormConf, Config.NIMBUS_FILE_COPY_EXPIRATION_SECS).get
  private val expiredCallback = new ExpiredCallback[String, WritableByteChannel] {
    override def expire(k: String, v: WritableByteChannel): Unit = {
      v.close()
    }
  }
  private val fileCacheMap = new TimeCacheMap[String, WritableByteChannel](expireSeconds, expiredCallback)

  override def submitTopology(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology): Unit = {
    submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE))
  }

  override def submitTopologyWithOpts(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology, options: SubmitOptions): Unit = {

    implicit val system = clientContext.system
    val gearpumpStormTopology = GearpumpStormTopology(name, topology, jsonConf)
    val stormConfig = gearpumpStormTopology.getStormConfig
    val processorGraph = GraphBuilder.build(gearpumpStormTopology)
    val config = UserConfig.empty
          .withValue[StormTopology](StormConstants.STORM_TOPOLOGY, topology)
          .withValue[JMap[AnyRef, AnyRef]](StormConstants.STORM_CONFIG, stormConfig)
    val app = StreamApplication(name, processorGraph, config)
    LOG.info(s"jar file uploaded to $uploadedJarLocation")
    val appId = clientContext.submit(app, uploadedJarLocation)
    applications += name -> appId
    topologies += name -> (topology, stormConfig)
    LOG.info(s"Storm Application $appId submitted")
  }

  override def killTopologyWithOpts(name: String, options: KillOptions): Unit = {
    clientContext.shutdown(applications.getOrElse(name, throw new RuntimeException(s"topology $name not found")))
    applications -= name
    topologies -= name
    LOG.info(s"Killed topology $name")
  }

  override def getNimbusConf: String = {
    JSONValue.toJSONString(stormConf)
  }

  override def getTopology(name: String): StormTopology = {
    updateApps
    topologies.getOrElse(name,
      throw new RuntimeException(s"topology $name not found"))._1
  }

  override def getTopologyConf(name: String): String = {
    updateApps
    JSONValue.toJSONString(topologies.getOrElse(name,
      throw new RuntimeException(s"topology $name not found"))._2)
  }

  override def getUserTopology(id: String): StormTopology = getTopology(id)

  override def beginFileUpload(): String = {
    val location = s"stormjar-${UUID.randomUUID()}.jar"
    val channel = Channels.newChannel(new FileOutputStream(location))
    fileCacheMap.put(location, channel)
    LOG.info(s"Uploading file from client to $location")
    location
  }

  override def uploadChunk(location: String, chunk: ByteBuffer): Unit = {
    if (!fileCacheMap.containsKey(location)) {
      throw new RuntimeException(s"File for $location does not exist (or timed out)")
    } else {
      val channel = fileCacheMap.get(location)
      channel.write(chunk)
      fileCacheMap.put(location, channel)
    }
  }

  override def finishFileUpload(location: String): Unit = {
    if (!fileCacheMap.containsKey(location)) {
      throw new RuntimeException(s"File for $location does not exist (or timed out)")
    } else {
      val channel = fileCacheMap.get(location)
      channel.close()
      fileCacheMap.remove(location)
      new File(location).delete()
    }
  }

  override def getClusterInfo: ClusterSummary = {
    updateApps
    val topologySummaryList = topologies.map { case (name, _) =>
      new TopologySummary(name, name, 0, 0, 0, 0, "")
    }.toSeq
    new ClusterSummary(List[SupervisorSummary]().asJava, 0, topologySummaryList.asJava)
  }

  override def beginFileDownload(file: String): String = {
    throw new UnsupportedOperationException
  }

  override def uploadNewCredentials(s: String, credentials: Credentials): Unit = {
    throw new UnsupportedOperationException
  }
    override def activate(name: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def rebalance(name: String, options: RebalanceOptions): Unit = {
    throw new UnsupportedOperationException
  }

  override def deactivate(name: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def getTopologyInfo(name: String): TopologyInfo = {
    throw new UnsupportedOperationException
  }

  override def getTopologyInfoWithOpts(s: String, getInfoOptions: GetInfoOptions): TopologyInfo = {
    throw new UnsupportedOperationException
  }

  override def killTopology(name: String): Unit = killTopologyWithOpts(name, new KillOptions())

  override def downloadChunk(name: String): ByteBuffer = {
    throw new UnsupportedOperationException
  }

  private def updateApps: Unit = {
    clientContext.listApps.appMasters.foreach { app =>
      val name = app.appName
      if (applications.contains(app.appName)) {
        if (app.status != MasterToAppMaster.AppMasterActive) {
          applications -= name
          topologies -= name
        }
      }
    }
  }
}


