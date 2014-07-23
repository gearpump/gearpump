package org.apache.gearpump.client

import akka.actor.{Props, Actor}
import org.apache.gearpump.task.TaskActor
import org.apache.gearpump.Partitioner
/**
 * Created by xzhong10 on 2014/7/23.
 */
class Split(conf : Map[String, Any], partitioner : Partitioner) extends TaskActor(conf, partitioner) {

  override   def onNext(msg : String) : Unit = {
    msg.split(" ").foreach(output(_))
  }
}
