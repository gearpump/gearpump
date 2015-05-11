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

package org.apache.gearpump.streaming.state.lib.op

import com.twitter.algebird.{AveragedValue, AveragedGroup}
import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.state.api.{StateSerializer, State, StateOp}
import org.apache.gearpump.streaming.state.lib.serializer.AveragedValueSerializer


class Average extends StateOp[AveragedValue] {
  val aggregate = (left: AveragedValue, right: AveragedValue) => {
    AveragedGroup.plus(left, right)
  }

  override def serializer: StateSerializer[AveragedValue] =
    new AveragedValueSerializer

  override def update(state: State[AveragedValue], message: Message): Unit = {
    val timestamp = message.timestamp
    val value = AveragedValue(message.msg.asInstanceOf[String].toLong)
    state.update(timestamp, value, aggregate)
  }
}
