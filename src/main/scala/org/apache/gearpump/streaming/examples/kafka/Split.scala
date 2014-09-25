package org.apache.gearpump.streaming.examples.kafka

import org.apache.gearpump.streaming.task.{TaskActor, Message}
import org.apache.gearpump.util.Configs

class Split(conf: Configs) extends TaskActor(conf) {

  override def onStart() : Unit = {
  }

  override def onNext(msg : Message) : Unit = {
    msg.msg.asInstanceOf[String].split(" ").foreach(
      word => output(new Message(word, System.currentTimeMillis())))
  }
}
