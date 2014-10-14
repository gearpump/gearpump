package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.Master
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, Configs}
import org.slf4j.{Logger, LoggerFactory}

object Gear extends App with ArgumentsParser {

  private val LOG: Logger = LoggerFactory.getLogger(Local.getClass)

  override val options: Array[(String, CLIOption[Any])] = Array()

  override  val remainArgs : Array[String] = Array("<kill|info|shell>")

  val config = parse(args)

  val command = config.remainArgs(0)

  val commandArgs = args.drop(1)

  command match {
    case "kill" =>
      Kill.main(commandArgs)
    case "shell" =>
      Shell.main(commandArgs)
    case "info" =>
      Info.main(commandArgs)
  }
}