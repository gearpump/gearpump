package org.apache.gearpump.client

import akka.actor.ActorSystem
import org.apache.gearpump.{SubmitApplication, AppDescription, Configs}
import org.apache.gearpump.service.SimpleKVService

/**
 * Created by xzhong10 on 2014/7/23.
 */
class Client {

  def start() : Unit = {
    String appMasterURL = SimpleKVService.get("appMaster")

    val system = ActorSystem("worker", Configs.SYSTEM_DEFAULT_CONFIG)

    val appMaster = system.actorSelection(appMasterURL)
    val app = createApplication()

    appMaster ! SubmitApplication(app)
  }

  def createApplication : AppDescription = {

  }
}

object Client {
  def main(args: Array[String]) {

    val kvService = argStrings(0);
    SimpleKVService.init(kvService)

    new Client().start();
  }
}
