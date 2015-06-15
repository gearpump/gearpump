package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.api.records._

object ContainerHelper extends TestConfiguration {

  def getContainer: Container = {
    getContainer(NodeId.newInstance(TEST_MASTER_HOSTNAME, 50000))
  }

  def getContainer(nodeId: NodeId): Container = {
    val token = Token.newInstance(Array[Byte](), "kind", Array[Byte](), "service")
    Container.newInstance(getContainerId, nodeId, "", Resource.newInstance(1024, 2), Priority.newInstance(1), token)
  }

  def getContainerId: ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), 1)
  }

  def getContainerId(id: Long): ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), id)
  }

  def getContainerStatus(id: Long, state: ContainerState): ContainerStatus = {
    ContainerStatus.newInstance(getContainerId(id), state, "", 0)
  }

}
