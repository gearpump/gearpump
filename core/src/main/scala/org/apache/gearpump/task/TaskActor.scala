package org.apache.gearpump.task

import akka.actor.Actor

/**
 * Created by xzhong10 on 2014/7/23.
 */
class TaskActor(val conf : Map[String, Any]) extends Actor {
  def receive : Receive = {
    case _ => Unit
  }

  def output(msg : Any) : Unit = {
    //TODO:
    // 1. use partioner to get partition of msg
    // 2. find the actor for this partition

    //send message to this actor

  }
}
