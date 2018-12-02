package io.gearpump.streaming.dsl.scalaapi

import akka.actor.ActorSystem
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.source.DataSourceTask
import io.gearpump.util.Graph
import io.gearpump.streaming.partitioner.PartitionerDescription
import io.gearpump.streaming.{ProcessorDescription, StreamApplication}
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration.Duration
class StreamAppSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "be able to generate multiple new streams" in {
    val context: ClientContext = mock[ClientContext]
    when(context.system).thenReturn(system)

    val dsl = StreamApp("dsl", context)
    dsl.source(List("A"), 2, "A") shouldBe a [Stream[_]]
    dsl.source(List("B"), 3, "B") shouldBe a [Stream[_]]

    val application = dsl.plan()
    application shouldBe a [StreamApplication]
    application.name shouldBe "dsl"
    val dag = application.userConfig
      .getValue[Graph[ProcessorDescription, PartitionerDescription]](StreamApplication.DAG).get
    dag.getVertices.size shouldBe 2
    dag.getVertices.foreach { processor =>
      processor.taskClass shouldBe classOf[DataSourceTask[_, _]].getName
      if (processor.description == "A") {
        processor.parallelism shouldBe 2
      } else if (processor.description == "B") {
        processor.parallelism shouldBe 3
      } else {
        fail(s"undefined source ${processor.description}")
      }
    }
  }
}
