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

package org.apache.gearpump.streaming.dsl.window.impl

import java.time.{Duration, Instant}

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction
import org.apache.gearpump.streaming.{Constants, MockUtil}
import org.apache.gearpump.streaming.dsl.plan.functions.FoldRunner
import org.apache.gearpump.streaming.dsl.window.api.SessionWindows
import org.apache.gearpump.streaming.source.Watermark
import org.mockito.Mockito.{times, verify}
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks


class DefaultWindowRunnerSpec extends PropSpec with PropertyChecks
  with Matchers with MockitoSugar {

  property("DefaultWindowRunner should handle SessionWindow") {

    val data = List(
      Message(("foo", 1L), Instant.ofEpochMilli(1L)),
      Message(("bar", 1L), Instant.ofEpochMilli(8L)),
      Message(("foo", 1L), Instant.ofEpochMilli(15L)),
      Message(("bar", 1L), Instant.ofEpochMilli(17L)),
      Message(("bar", 1L), Instant.ofEpochMilli(18L)),
      Message(("foo", 1L), Instant.ofEpochMilli(25L)),
      Message(("foo", 1L), Instant.ofEpochMilli(26L)),
      Message(("bar", 1L), Instant.ofEpochMilli(30L)),
      Message(("bar", 1L), Instant.ofEpochMilli(31L))
    )

    type KV = (String, Long)
    val taskContext = MockUtil.mockTaskContext
    implicit val system = MockUtil.system
    val reduce = ReduceFunction[KV]((kv1, kv2) => (kv1._1, kv1._2 + kv2._2))
    val operator = new FoldRunner(reduce, "reduce")
    val userConfig = UserConfig.empty.withValue(
      Constants.GEARPUMP_STREAMING_OPERATOR, operator)
    val windows = SessionWindows.apply[KV](Duration.ofMillis(4L))
    val groupBy = GroupAlsoByWindow[KV, String](_._1, windows)
    val windowRunner = new DefaultWindowRunner(taskContext, userConfig, groupBy)

    data.foreach(windowRunner.process)
    windowRunner.trigger(Watermark.MAX)

    verify(taskContext, times(2)).output(Message(Some(("foo", 1)), Watermark.MAX))
    verify(taskContext).output(Message(Some(("foo", 2)), Watermark.MAX))
    verify(taskContext, times(2)).output(Message(Some(("bar", 2)), Watermark.MAX))
    verify(taskContext).output(Message(Some(("bar", 1)), Watermark.MAX))
  }

}
