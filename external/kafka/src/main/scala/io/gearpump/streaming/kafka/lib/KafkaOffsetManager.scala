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

package io.gearpump.streaming.kafka.lib

import scala.util.{Failure, Success, Try}

import com.twitter.bijection.Injection
import org.slf4j.Logger

import io.gearpump._
import io.gearpump.streaming.transaction.api.OffsetStorage.{Overflow, StorageEmpty, Underflow}
import io.gearpump.streaming.transaction.api.{OffsetManager, OffsetStorage}
import io.gearpump.util.LogUtil

object KafkaOffsetManager {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaOffsetManager])
}

private[kafka] class KafkaOffsetManager(storage: OffsetStorage) extends OffsetManager {
  import io.gearpump.streaming.kafka.lib.KafkaOffsetManager._

  var maxTime: TimeStamp = 0L

  override def filter(messageAndOffset: (Message, Long)): Option[Message] = {
    val (message, offset) = messageAndOffset
    if (message.timestamp > maxTime) {
      maxTime = message.timestamp
      storage.append(maxTime, Injection[Long, Array[Byte]](offset))
    }
    Some(message)
  }

  override def resolveOffset(time: TimeStamp): Try[Long] = {
    storage.lookUp(time) match {
      case Success(offset) => Injection.invert[Long, Array[Byte]](offset)
      case Failure(Overflow(max)) =>
        LOG.warn(s"start time larger than the max stored TimeStamp; set to max offset")
        Injection.invert[Long, Array[Byte]](max)
      case Failure(Underflow(min)) =>
        LOG.warn(s"start time less than the min stored TimeStamp; set to min offset")
        Injection.invert[Long, Array[Byte]](min)
      case Failure(StorageEmpty) => Failure(StorageEmpty)
      case Failure(e) => throw e
    }
  }

  override def close(): Unit = {
    storage.close()
  }
}
