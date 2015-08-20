package io.gearpump.util

import io.gearpump.cluster.ClusterConfig

import scala.util.Try


trait AkkaApp {

  type Config = com.typesafe.config.Config

  def main(akkaConf: Config, args: Array[String]): Unit

  def help: Unit

  protected def akkaConfig: Config = {
    ClusterConfig.load.default
  }

  def main(args: Array[String]): Unit = {
    Try {
      main(akkaConfig, args)
    }.failed.foreach{ex => help; throw ex}
  }
}
