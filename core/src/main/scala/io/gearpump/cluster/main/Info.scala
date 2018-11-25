package io.gearpump.cluster.main

import io.gearpump.cluster.MasterToAppMaster.AppMastersData
import io.gearpump.cluster.client.ClientContext
import io.gearpump.util.MasterClientCommand

/** Tool to query master info */
object Info extends MasterClientCommand with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    // For document purpose only, OPTION_CONFIG option is not used here.
    // OPTION_CONFIG is parsed by parent shell command "Gear" transparently.
    Gear.OPTION_CONFIG -> CLIOption("custom configuration file", required = false,
      defaultValue = None))

  override val description = "Query the Application list"

  // scalastyle:off println
  def main(akkaConf: Config, args: Array[String]): Unit = {
    val client = ClientContext(akkaConf)

    val AppMastersData(appMasters) = client.listApps
    Console.println("== Application Information ==")
    Console.println("====================================")
    appMasters.foreach { appData =>
      Console.println(s"application: ${appData.appId}, name: ${appData.appName}, " +
        s"status: ${appData.status}, worker: ${appData.workerPath}")
    }
    client.close()
  }
  // scalastyle:on println
}
