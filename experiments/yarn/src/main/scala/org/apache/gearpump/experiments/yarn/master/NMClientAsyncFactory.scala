package org.apache.gearpump.experiments.yarn.master

import org.apache.gearpump.experiments.yarn.NodeManagerCallbackHandler
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl


trait NMClientAsyncFactory {
  def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync
}

object NMClientAsyncFactory  {

  val LOG: Logger = LogUtil.getLogger(getClass)

  val instance: NMClientAsyncFactory = new NMClientAsyncFactory {

    override def newInstance(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync = {
      LOG.info("Creating NMClientAsync")
      val nmClient = new NMClientAsyncImpl(containerListener)
      nmClient.init(yarnConf)
      nmClient.start()
      nmClient
    }
  }
  def apply():NMClientAsyncFactory = instance
}
