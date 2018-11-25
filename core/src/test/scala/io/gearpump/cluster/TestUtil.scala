package io.gearpump.cluster

object TestUtil {
  val DEFAULT_CONFIG = ClusterConfig.default("test.conf")
  val MASTER_CONFIG = ClusterConfig.master("test.conf")
  val UI_CONFIG = ClusterConfig.ui("test.conf")

  class DummyAppMaster(context: AppMasterContext, app: AppDescription) extends ApplicationMaster {
    context.masterProxy !(context, app)

    def receive: Receive = null
  }

  val dummyApp: AppDescription =
    AppDescription("dummy", classOf[DummyAppMaster].getName, UserConfig.empty)
}
