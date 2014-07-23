package org.apache.gearpump.client

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.task.TaskActor
import org.apache.gearpump.Partitioner
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.HashMap

/**
 * Created by xzhong10 on 2014/7/23.
 */
class Sum(conf : Map[String, Any], partitioner : Partitioner) extends TaskActor(conf, partitioner) {
  import Sum._

  private val map : HashMap[String, Int] = new HashMap[String, Int]()
  private var scheduler : Cancellable = null

  override def preStart() : Unit = {
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(15, TimeUnit.SECONDS))(reportWordCount)
  }

  override def onNext(msg : String) : Unit = {
    if (null == msg) {
      return
    }
    val current = map.getOrElse(msg, 0)
    map.put(msg, current + 1)
  }

  override def onStop() : Unit = {
    scheduler.cancel()
  }

  def reportWordCount : Unit = {
    LOG.info("Reporting WordCount...")
    for (kv  <- map) {
      LOG.info(kv.toString())
    }
  }
}

object Sum {
  private val LOG: Logger = LoggerFactory.getLogger(Sum.getClass)
}
