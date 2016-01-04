package io.gearpump.streaming.task

import io.gearpump._
import io.gearpump.metrics.Metrics
import io.gearpump.util.LogUtil
import org.slf4j.Logger

/**
  * Output port for a task
  */
class OutputPort(task: TaskActor, portName: String, subscribers: List[Subscriber]) extends ExpressTransport {
  val appId = task.taskContextData.appId
  val executorId = task.taskContextData.executorId
  val taskId = task.taskId

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId,
                  executor = executorId, task = taskId)

  var subscriptions = List.empty[(Int, Subscription)]

  val sendThroughput = Metrics(task.context.system).meter(s"${task.metricName}.port$portName:sendThroughput")

  /**
    * output to a downstream by specifying a arrayIndex
    * @param arrayIndex, subscription index (not ProcessorId)
    * @param msg
    */
  def output(arrayIndex: Int, msg: Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    count +=  this.subscriptions(arrayIndex)._2.sendMessage(msg)
    sendThroughput.mark(count)
  }

  def output(msg : Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    this.subscriptions.foreach{ subscription =>
      count += subscription._2.sendMessage(msg)
    }
    sendThroughput.mark(count)
  }

  def initialize(sessionId: Int, maxPendingMessageCount: Int, ackOnceEveryMessageCount: Int): Unit = {
    subscriptions = subscribers.map { subscriber =>
      (subscriber.processorId ,
        new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
          maxPendingMessageCount, ackOnceEveryMessageCount))
    }.sortBy(_._1)

    subscriptions.foreach(_._2.start)
  }

  def minClock: TimeStamp = {
    this.subscriptions.foldLeft(Long.MaxValue){ (clock, subscription) =>
      Math.min(clock, subscription._2.minClock)
    }
  }

  def allowSendingMoreMessages(): Boolean = {
    subscriptions.forall(_._2.allowSendingMoreMessages())
  }

  def receiveAck(ack: Ack): Unit = {
    subscriptions.find(_._1 == ack.taskId.processorId).foreach(_._2.receiveAck(ack))
  }
}
