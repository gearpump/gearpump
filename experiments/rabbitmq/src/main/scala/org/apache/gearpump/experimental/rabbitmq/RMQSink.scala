/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.experimental.rabbitmq

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import com.rabbitmq.client.Channel
import com.rabbitmq.client.{Connection, ConnectionFactory}
import org.apache.gearpump.util.LogUtil

class RMQSink(userConfig: UserConfig,
    val connFactory: (UserConfig) => ConnectionFactory) extends DataSink{

  private val LOG = LogUtil.getLogger(getClass)
  var connectionFactory: ConnectionFactory = connFactory(userConfig)
  var connection: Connection = null
  var channel: Channel = null
  var queueName: String = null

  def this(userConfig: UserConfig) = {
    this(userConfig, RMQSink.getConnectionFactory)
  }

  override def open(context: TaskContext): Unit = {
    connection = connectionFactory.newConnection
    channel = connection.createChannel
    if (channel == null) {
      throw new RuntimeException("None of RabbitMQ channels are available.")
    }
    setupQueue()
  }

  override def write(message: Message): Unit = {
    publish(message.msg)
  }

  override def close(): Unit = {
    channel.close()
    connection.close()
  }

  protected def setupQueue(): Unit = {
    val queue = RMQSink.getQueueName(userConfig)
    if (queue.isEmpty) {
      throw new RuntimeException("can not get a RabbitMQ queue name")
    }

    queueName = queue.get
    channel.queueDeclare(queue.get, false, false, false, null)
  }

  def publish(msg: Any): Unit = {
    msg match {
      case seq: Seq[Any] =>
        seq.foreach(publish)
      case str: String => {
        channel.basicPublish("", queueName, null, msg.asInstanceOf[String].getBytes)
      }
      case byteArray: Array[Byte] => {
        channel.basicPublish("", queueName, null, byteArray)
      }
      case _ => {
        LOG.warn("matched unsupported message!")
      }
    }
  }

}

object RMQSink {

  val RMQSINK = "rmqsink"
  val QUEUE_NAME = "rabbitmq.queue.name"
  val SERVER_HOST = "rabbitmq.connection.host"
  val SERVER_PORT = "rabbitmq.connection.port"
  val CONNECTION_URI = "rabbitmq.connection.uri"
  val VIRTUAL_HOST = "rabbitmq.virtualhost"
  val AUTH_USERNAME = "rabbitmq.auth.username"
  val AUTH_PASSWORD = "rabbitmq.auth.password"
  val AUTOMATIC_RECOVERY = "rabbitmq.automatic.recovery"
  val CONNECTION_TIMEOUT = "rabbitmq.connection.timeout"
  val NETWORK_RECOVERY_INTERVAL = "rabbitmq.network.recovery.interval"
  val REQUESTED_HEARTBEAT = "rabbitmq.requested.heartbeat"
  val TOPOLOGY_RECOVERY_ENABLED = "rabbitmq.topology.recoveryenabled"
  val REQUESTED_CHANNEL_MAX = "rabbitmq.channel.max"
  val REQUESTED_FRAME_MAX = "rabbitmq.frame.max"

  def getConnectionFactory(userConfig : UserConfig): ConnectionFactory = {
    val factory : ConnectionFactory = new ConnectionFactory

    val uri : Option[String] = userConfig.getString(CONNECTION_URI)
    if (uri.nonEmpty) {
      factory.setUri(uri.get)
    } else {
      val serverHost : Option[String] = userConfig.getString(SERVER_HOST)
      val serverPort : Option[Int] = userConfig.getInt(SERVER_PORT)
      if (!serverHost.nonEmpty) {
        throw new RuntimeException("missed config key : " + SERVER_HOST)
      }

      if (!serverPort.nonEmpty) {
        throw new RuntimeException("missed config key : " + SERVER_PORT)
      }

      factory.setHost(serverHost.get)
      factory.setPort(serverPort.get)
    }

    val virtualHost : Option[String] = userConfig.getString(VIRTUAL_HOST)
    if (virtualHost.nonEmpty) {
      factory.setVirtualHost(virtualHost.get)
    }

    val authUserName : Option[String] = userConfig.getString(AUTH_USERNAME)
    if (authUserName.nonEmpty) {
      factory.setUsername(authUserName.get)
    }

    val authPassword : Option[String] = userConfig.getString(AUTH_PASSWORD)
    if (authPassword.nonEmpty) {
      factory.setPassword(authPassword.get)
    }

    val automaticRecovery : Option[Boolean] = userConfig.getBoolean(AUTOMATIC_RECOVERY)
    if (automaticRecovery.nonEmpty) {
      factory.setAutomaticRecoveryEnabled(automaticRecovery.get)
    }

    val connectionTimeOut : Option[Int] = userConfig.getInt(CONNECTION_TIMEOUT)
    if (connectionTimeOut.nonEmpty) {
      factory.setConnectionTimeout(connectionTimeOut.get)
    }

    val networkRecoveryInterval : Option[Int] = userConfig.getInt(NETWORK_RECOVERY_INTERVAL)
    if (networkRecoveryInterval.nonEmpty) {
      factory.setNetworkRecoveryInterval(networkRecoveryInterval.get)
    }

    val requestedHeartBeat : Option[Int] = userConfig.getInt(REQUESTED_HEARTBEAT)
    if (requestedHeartBeat.nonEmpty) {
      factory.setRequestedHeartbeat(requestedHeartBeat.get)
    }

    val topologyRecoveryEnabled : Option[Boolean] = userConfig.getBoolean(TOPOLOGY_RECOVERY_ENABLED)
    if (topologyRecoveryEnabled.nonEmpty) {
      factory.setTopologyRecoveryEnabled(topologyRecoveryEnabled.get)
    }

    val requestedChannelMax : Option[Int] = userConfig.getInt(REQUESTED_CHANNEL_MAX)
    if (requestedChannelMax.nonEmpty) {
      factory.setRequestedChannelMax(requestedChannelMax.get)
    }

    val requestedFrameMax : Option[Int] = userConfig.getInt(REQUESTED_FRAME_MAX)
    if (requestedFrameMax.nonEmpty) {
      factory.setRequestedFrameMax(requestedFrameMax.get)
    }

    factory
  }

  def getQueueName(userConfig: UserConfig): Option[String] = {
    userConfig.getString(QUEUE_NAME)
  }

}
