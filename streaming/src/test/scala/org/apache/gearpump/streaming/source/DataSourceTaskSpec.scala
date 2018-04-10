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

package org.apache.gearpump.streaming.source

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.window.impl.{TimestampedValue, TriggeredOutputs, StreamingOperator}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class DataSourceTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("DataSourceTask should setup data source") {
    forAll(Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) {
      (startTime: Instant) =>
      val taskContext = MockUtil.mockTaskContext
      implicit val system = MockUtil.system
      val dataSource = mock[DataSource]
      val config = UserConfig.empty
        .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
        val runner = mock[StreamingOperator[Any, Any]]
      val sourceTask = new DataSourceTask[Any, Any](dataSource, runner, taskContext, config)

      sourceTask.onStart(startTime)

      verify(dataSource).open(taskContext, startTime)
    }
  }

  property("DataSourceTask should read from DataSource and transform inputs") {
    forAll(Gen.alphaStr, Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) {
      (str: String, timestamp: Instant) =>
        val taskContext = MockUtil.mockTaskContext
        implicit val system = MockUtil.system
        val dataSource = mock[DataSource]
        val config = UserConfig.empty
          .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
        val processor = mock[StreamingOperator[String, String]]
        val sourceTask = new DataSourceTask[String, String](dataSource, processor,
          taskContext, config)
        val msg = Message(str, timestamp)
        when(dataSource.read()).thenReturn(msg)

        when(processor.flatMap(new TimestampedValue[String](msg))).thenReturn(
          Some(new TimestampedValue[String](msg))
        )
        when(processor.trigger(Watermark.MAX)).thenReturn(
          TriggeredOutputs[String](None, Watermark.MAX))

        sourceTask.onNext(Message("next"))
        sourceTask.onWatermarkProgress(Watermark.MAX)

        verify(taskContext).output(msg)
        verify(taskContext).updateWatermark(Watermark.MAX)
    }
  }

  property("DataSourceTask should teardown DataSource") {
    val taskContext = MockUtil.mockTaskContext
    implicit val system = MockUtil.system
    val dataSource = mock[DataSource]
    val config = UserConfig.empty
      .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
    val runner = mock[StreamingOperator[Any, Any]]
    val sourceTask = new DataSourceTask[Any, Any](dataSource, runner, taskContext, config)

    sourceTask.onStop()

    verify(dataSource).close()
  }
}
