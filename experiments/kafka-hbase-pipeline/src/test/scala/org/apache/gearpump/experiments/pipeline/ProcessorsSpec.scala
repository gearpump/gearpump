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
package org.apache.gearpump.experiments.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.hbase.HBaseSink._
import org.apache.gearpump.experiments.hbase.{HBaseRepo, HBaseSinkInterface}
import org.apache.gearpump.experiments.pipeline.Messages.{Body, Datum, Envelope, _}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.task.StartTime
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.slf4j.Logger
import upickle._

object Processors {
  val LOG: Logger = LogUtil.getLogger(getClass)

  implicit val system = MockUtil.system

  val pipelineConfigText =
    """
      |pipeline {
      |  cpu.interval = 50
      |  memory.interval = 20
      |  processors = 1
      |  persistors = 1
      |}
      |hbase {
      |  zookeeper.connect = "127.0.0.1"
      |  table {
      |    name = "pipeline"
      |    column {
      |      family = "metrics"
      |      name = "average"
      |    }
      |  }
      |}
    """.
      stripMargin
  val hbaseCPU = Mockito.mock(classOf[HBaseSinkInterface])
  val hbaseMEM = Mockito.mock(classOf[HBaseSinkInterface])
  val repoCPU = new HBaseRepo {
    def getHBase(table:String, conf:Configuration): HBaseSinkInterface = hbaseCPU
  }
  val repoMEM = new HBaseRepo {
    def getHBase(table:String, conf:Configuration): HBaseSinkInterface = hbaseMEM
  }
  val pipelineConfig = PipeLineConfig(ConfigFactory.parseString(pipelineConfigText))
  val userConfig = UserConfig.empty.withValue(PIPELINE, pipelineConfig)
  val cpuConfig = userConfig.withValue(HBASESINK,repoCPU)
  val memoryConfig = userConfig.withValue(HBASESINK,repoMEM)

  val data = Array[String](
    """
      |{"id":"2a329674-12ad-49f7-b40d-6485aae0aae8","on":"2015-04-02T18:52:02.680178753Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-13\",\"event_ts\":\"2015-04-02T18:52:04.784086993Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993997},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39018},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """
      .stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin
  )

  val timeStamp = 1428004406134L
  val timeStamps = Array[Long](timeStamp, timeStamp + 30)

  def getMetrics(data: String): Array[Datum] = {
    val envelope = read[Envelope](data)
    val body = read[Body](envelope.body)
    body.metrics
  }

}

class CpuProcessorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  import Processors._

  val context = MockUtil.mockTaskContext

  val cpuProcessor = new CpuProcessor(context, userConfig)

  property("CpuProcessor should send an empty Array[Datum]") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum]()), timeStamps(0))
    cpuProcessor.onStart(StartTime())
    cpuProcessor.onNext(Message(write[Array[Datum]](metrics), timeStamps(0)))
    verify(context).output(expected)
  }

  property("CpuProcessor should send an Array[Datum](Datum(\"CPU\", \"total\", 1.401257775E7)") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum](Datum("CPU", "total", 1.401257775E7))), timeStamps(1))
    cpuProcessor.onNext(Message(write[Array[Datum]](metrics), timeStamps(1)))
    verify(context).output(expected)
  }

  after(() => {
    cpuProcessor.onStop()
  })
}

class MemoryProcessorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  import Processors._

  val context = MockUtil.mockTaskContext

  val memoryProcessor = new MemoryProcessor(context, userConfig)

  property("Memory should send an empty Array[Datum](Datum(\"MEM\",\"free\",1.0459182421333334E10))") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum]()), timeStamps(0))
    memoryProcessor.onStart(StartTime())
    memoryProcessor.onNext(Message(write[Array[Datum]](metrics), timeStamps(0)))
    verify(context).output(expected)
  }

  property("MemoryProcessor should send an Array[Datum]") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum](Datum("MEM","free",1.0459182421333334E10))), timeStamps(1))
    memoryProcessor.onNext(Message(write[Array[Datum]](metrics), timeStamps(1)))
    verify(context).output(expected)
  }

  after(() => {
    memoryProcessor.onStop()
  })
}

class CpuPersistorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  import Processors._

  val context = MockUtil.mockTaskContext
  val cpuPersistor = new CpuPersistor(context, cpuConfig)

  property("CpuPersistor should call HBaseSinkInterface.insert(\"1428004406134\", \"metrics\", \"average\", \"{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993997}\"") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum]()), timeStamps(0))
    cpuPersistor.onStart(StartTime())
    cpuPersistor.onNext(Message(write[Array[Datum]](metrics), timeStamps(0)))
    val metric = """|{"dimension":"CPU","metric":"total","value":27993997}""".stripMargin
    verify(hbaseCPU).insert("1428004406134", "metrics", "average", metric)
  }

  after(() => {
    cpuPersistor.onStop()
  })
}

class MemoryPersistorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  import Processors._

  val context = MockUtil.mockTaskContext
  val memoryPersistor = new MemoryPersistor(context, memoryConfig)

  property("MemoryPersistor should call HBaseSinkInterface.insert(\"1428004406134\", \"metrics\", \"average\", \"{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993997}\"") {
    val metrics = getMetrics(data(0))
    val expected = Message(write[Array[Datum]](Array[Datum]()), timeStamps(0))
    memoryPersistor.onStart(StartTime())
    memoryPersistor.onNext(Message(write[Array[Datum]](metrics), timeStamps(0)))
    val metric = """|{"dimension":"CPU","metric":"total","value":27993997}""".stripMargin
    verify(hbaseMEM).insert("1428004406134", "metrics", "average", metric)
  }

  after(() => {
    memoryPersistor.onStop()
  })
}




