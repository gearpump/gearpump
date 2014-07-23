package org.apache.gearpump.client

import akka.actor.{Props, ActorSystem}
import org.apache.gearpump._
import org.apache.gearpump.service.SimpleKVService

/**
 * Created by xzhong10 on 2014/7/23.
 */
class Client {

  def start() : Unit = {
    val appMasterURL = SimpleKVService.get("appMaster")

    val system = ActorSystem("worker", Configs.SYSTEM_DEFAULT_CONFIG)

    val appMaster = system.actorSelection(appMasterURL)
    val app = createApplication()

    appMaster ! SubmitApplication(app)
  }

  def createApplication() : AppDescription = {
    val config = Map()
    val partitioner = new HashPartitioner()
    val split = TaskDescription(Props(classOf[Split], config, partitioner))
    val sum = TaskDescription(Props(classOf[Sum], config, partitioner))
    val app = AppDescription("wordCount", Array(StageDescription(split, 1), StageDescription(sum, 1)))

    app

  }
}

object Client {
  def main(args: Array[String]) {

    val kvService = args(0);
    SimpleKVService.init(kvService)

    new Client().start();
  }
}
