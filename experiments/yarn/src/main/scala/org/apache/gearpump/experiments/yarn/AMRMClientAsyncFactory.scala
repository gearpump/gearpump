package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync

trait AMRMClientAsyncFactory[T <: ContainerRequest] {
  def newIntance(intervalMs: Int, callbackHandler: AMRMClientAsync.CallbackHandler):AMRMClientAsync[T]
}

object AMRMClientAsyncFactory {
  val instance = new AMRMClientAsyncFactory[ContainerRequest] {
    override def newIntance(intervalMs: Int, callbackHandler: AMRMClientAsync.CallbackHandler):AMRMClientAsync[ContainerRequest] = {
      AMRMClientAsync.createAMRMClientAsync(intervalMs, callbackHandler)
    }
  }
  def apply() = instance
}
