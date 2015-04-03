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

package org.apache.gearpump.streaming.task

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}

import scala.util.Try

/**
 * Base class for StreamingProducer Tasks
 */
abstract class StreamProducerTask(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  protected val getProcessorKeyPrefix: String = "processor."+this.getClass.getName+"."

  protected val msgDecoder: MessageDecoder = getInstance[MessageDecoder](getProcessorKeyPrefix+"decoder").getOrElse(MessageDecoder())
  protected val filter: TimeStampFilter = getInstance[TimeStampFilter](getProcessorKeyPrefix+"filter").getOrElse(null)

  private def getInstance[C](key: String): Option[C] = {
    conf.getString(key).map(Class.forName(_).newInstance().asInstanceOf[C])
  }
}
