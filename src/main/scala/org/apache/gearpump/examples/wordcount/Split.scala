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

package org.apache.gearpump.examples.wordcount

import org.apache.gearpump.task.{Message, TaskActor}
import org.apache.gears.cluster.Configs

class Split(conf : Configs) extends TaskActor(conf) {

  private val txt =
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


  override def onStart() : Unit = {
    self ! Message(System.currentTimeMillis(), "start")
  }

  override def onNext(msg : String) : Unit = {
    txt.lines.foreach((line) => line.split(" ").foreach(output(_)))
    self ! Message(System.currentTimeMillis(), "continue")
  }
}
