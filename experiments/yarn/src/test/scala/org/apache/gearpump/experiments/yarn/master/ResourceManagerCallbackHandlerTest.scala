package org.apache.gearpump.experiments.yarn.master

import akka.actor.ActorSystem
import org.apache.gearpump.experiments.yarn.TestConfiguration
import org.apache.gearpump.experiments.yarn.ContainerHelper._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol.{RMShutdownRequest, RMError, ContainersCompleted, ContainersAllocated}
import org.apache.hadoop.yarn.api.records._
import org.scalatest.FlatSpecLike
import org.specs2.mock.Mockito
import akka.testkit._
import scala.collection.JavaConverters._
import org.scalatest.Matchers._

class ResourceManagerCallbackHandlerTest extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with Mockito
with TestConfiguration {

  "The ResourceManagerCallbackHandler" should
    "send ContainersAllocated message to ResourceManagerClient when onContainersAllocated method is called" in {
    val probe = TestProbe()
    val handler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val containers = List[Container](getContainer)
    handler.onContainersAllocated(containers.asJava)
    probe.expectMsg(ContainersAllocated(containers))
  }

  it should "send ContainersCompleted message to ResourceManagerClient when onContainersCompleted method is called" in {
    val probe = TestProbe()
    val handler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val statuses = List[ContainerStatus](getContainerStatus(1, ContainerState.COMPLETE))
    handler.onContainersCompleted(statuses.asJava)
    probe.expectMsg(ContainersCompleted(statuses))
  }

  it should "send RMError message to ResourceManagerClient when onError method is called" in {
    val probe = TestProbe()
    val handler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val throwable = new RuntimeException
    handler.onError(throwable)
    probe.expectMsg(RMError(throwable))
  }

  it should "send RMShutdownRequest message to ResourceManagerClient when onShutdownRequest method is called" in {
    val probe = TestProbe()
    val handler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    handler.onShutdownRequest
    probe.expectMsg(RMShutdownRequest)
  }

  it should "return 0.5 when getProgress method it called" in {
    val probe = TestProbe()
    val handler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    handler.getProgress should be(0.5f)
  }
}
