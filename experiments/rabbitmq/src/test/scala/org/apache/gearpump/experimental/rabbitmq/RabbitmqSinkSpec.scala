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

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class RabbitmqSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("RMQSink should insert a row successfully") {

    val taskContext = mock[TaskContext]

    val map = Map[String, String]("rabbitmq.queue.name" -> "test",
    "rabbitmq.connection.host" -> "localhost",
    "rabbitmq.connection.port" -> "5672")
    val userConfig = new UserConfig(map)

    val rmqSink = new RMQSink(userConfig)

    assert(RMQSink.getQueueName(userConfig).get == "test")

//    rmqSink.open(taskContext)

//    var msg: String = "{ 'hello' : 'world' }"
//    rmqSink.publish(msg)

//    rmqSink.close()
  }

}
