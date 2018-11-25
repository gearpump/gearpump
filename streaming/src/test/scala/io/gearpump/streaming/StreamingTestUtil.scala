package io.gearpump.streaming

import akka.actor.{ActorRef, Props}
import akka.testkit.TestActorRef
import io.gearpump.util.Graph
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster.{AppDescription, AppMasterContext, MiniCluster, UserConfig}
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.appmaster.AppMaster

object StreamingTestUtil {
  private var executorId = 0
  val testUserName = "testuser"

  def startAppMaster(miniCluster: MiniCluster, appId: Int): TestActorRef[AppMaster] = {

    implicit val actorSystem = miniCluster.system
    val masterConf = AppMasterContext(appId, testUserName, Resource(1), null,
      None, miniCluster.mockMaster)

    val app = StreamApplication("test", Graph.empty, UserConfig.empty)
    val appDescription = AppDescription(app.name, app.appMaster.getName, app.userConfig)
    val props = Props(new AppMaster(masterConf, appDescription))
    val appMaster = miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
    val registerAppMaster = RegisterAppMaster(appId, ActorRef.noSender, null)
    miniCluster.mockMaster.tell(registerAppMaster, appMaster)

    appMaster
  }
}
