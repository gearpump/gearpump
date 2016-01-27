package io.gearpump.util

import io.gearpump.cluster.ClusterConfig

import scala.util.Try


/**
 * A Main class helper to load Akka configuration automatically. 
 * Should this provide an actor system as well ? An AS is requested by the external connectors, usually assembled in the main app.
 */
trait AkkaApp {

  type Config = com.typesafe.config.Config

  def main(akkaConf: Config, args: Array[String]): Unit

  def help: Unit

  protected def akkaConfig: Config = {
    ClusterConfig.default()
  }

  def main(args: Array[String]): Unit = {
    Try {
      main(akkaConfig, args)
    }.failed.foreach{ex => help; throw ex}
  }
}
