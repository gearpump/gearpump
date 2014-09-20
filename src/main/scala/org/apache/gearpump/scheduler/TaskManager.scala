package org.apache.gearpump.scheduler

import akka.actor.ActorRef
import org.apache.gearpump.streaming.AppDescription
import org.apache.gearpump.util.Configs

import scala.collection.mutable

class TaskManager(val config : Configs, val appDescription : AppDescription,
                  var workers : mutable.HashMap[ActorRef, Int]) {

}

object TaskManager{
  def apply(config : Configs, app : AppDescription, workers : mutable.HashMap[ActorRef, Int]) = new TaskManager(config, app, workers)
}
