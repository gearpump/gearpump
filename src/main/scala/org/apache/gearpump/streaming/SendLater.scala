package org.apache.gearpump.streaming

import java.util

import akka.actor.{Stash, Actor}
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.Express
import org.apache.gearpump.transport.netty.TaskMessage
import akka.pattern.pipe
import org.slf4j.{LoggerFactory, Logger}

case object TaskLocationReady

class SendLater extends Actor {
  import SendLater.LOG

  import context.dispatcher

  private[this] val queue : util.ArrayDeque[TaskMessage] = new util.ArrayDeque[TaskMessage](SendLater.INITIAL_WINDOW_SIZE)

  private var taskLocationReady = false

  val express = Express(context.system)

  def receive : Receive = {
    case TaskLocations(locations) => {
      val result = locations.flatMap { kv =>
        val (host, set) = kv
        set.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.remoteAddressMap.send(result)
      express.remoteAddressMap.future().map(_ => TaskLocationReady).pipeTo(self)
    }
    case msg : TaskMessage => {
      queue.add(msg)
      if (taskLocationReady) {
        sendPendingMessages()
      }
    }
    case TaskLocationReady => {
      taskLocationReady = true
      sendPendingMessages()
    }
  }

  def sendPendingMessages() : Unit = {
    var done = false
    while (!done) {
      val msg = queue.poll()
      if (msg != null) {
        val host = express.remoteAddressMap.get().get(msg.task())
        if (host.isDefined) {
          express.transport(msg, host.get)
        } else {
          LOG.error(s"Cannot find taskId ${TaskId.fromLong(msg.task())} in remoteAddressMap, dropping it...")
        }
      } else {
        done = true
      }
    }
  }
}

object SendLater {
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val LOG: Logger = LoggerFactory.getLogger(classOf[SendLater])
 }