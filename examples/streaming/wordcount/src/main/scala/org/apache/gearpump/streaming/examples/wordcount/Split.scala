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

package org.apache.gearpump.streaming.examples.wordcount

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

class Split(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.{output, self}

  override def onStart(startTime : StartTime) : Unit = {
    self ! Message("start")
  }

  override def onNext(msg : Message) : Unit = {
    Split.TEXT_TO_SPLIT.lines.foreach { line =>
      line.split("[\\s]+").filter(_.nonEmpty).foreach { msg =>
        output(new Message(msg, System.currentTimeMillis()))
      }
    }
    self ! Message("continue", System.currentTimeMillis())
  }
}

object Split {
  val TEXT_TO_SPLIT =
    """
      |   Licensed to the Apache Software Foundation (ASF) under one
      |   or more contributor license agreements.  See the NOTICE file
      |   distributed with this work for additional information
      |   regarding copyright ownership.  The ASF licenses this file
      |   to you under the Apache License, Version 2.0 (the
      |   "License"); you may not use this file except in compliance
      |   with the License.  You may obtain a copy of the License at
      |
      |       http://www.apache.org/licenses/LICENSE-2.0
      |
      |   Unless required by applicable law or agreed to in writing, software
      |   distributed under the License is distributed on an "AS IS" BASIS,
      |   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      |   See the License for the specific language governing permissions and
      |   limitations under the License.
    """.stripMargin
}