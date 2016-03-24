/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.examples.streamingkmeans

import java.util.concurrent.LinkedBlockingQueue

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.collection.mutable
import scala.util.Random

class ClusterDistribution(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  private[streamingkmeans] val dataQueue: LinkedBlockingQueue[List[Double]] = new LinkedBlockingQueue[List[Double]]()
  private[streamingkmeans] var isBegin: Boolean = true

  private val decayFactor = conf.getDouble("decayFactor").get
  private val dimension = conf.getInt("dimension").get

  private[streamingkmeans] val center: Array[Double] = new Array[Double](dimension)
  private[streamingkmeans] val points: mutable.MutableList[List[Double]] = new mutable.MutableList()
  private[streamingkmeans] var previousNumber = 0
  private[streamingkmeans] var currentNumber = 0


  /**
   * init center randomly
   */
  private[streamingkmeans] def initCenter(): Unit = {
    val random = new Random()
    for (i <- center.indices) {
      center.update(i, random.nextGaussian())
    }
  }

  /**
   * The update algorithm uses the "mini-batch" KMeans rule,
   * generalized to incorporate forgetfullness (i.e. decay).
   * The update rule (for each cluster) is:
   *
   * {{{
   * c_t+1 = [(c_t * n_t * a) + (x_t * m_t)] / [n_t + m_t]
   * n_t+t = n_t * a + m_t
   * }}}
   *
   * Where c_t is the previously estimated centroid for that cluster,
   * n_t is the number of points assigned to it thus far, x_t is the centroid
   * estimated on the current batch, and m_t is the number of points assigned
   * to that centroid in the current batch.
   *
   * The decay factor 'a' scales the contribution of the clusters as estimated thus far,
   * by applying a as a discount weighting on the current point when evaluating
   * new incoming data. If a=1, all batches are weighted equally. If a=0, new centroids
   * are determined entirely by recent data. Lower values correspond to
   * more forgetting.
   */
  private[streamingkmeans] def updateCenter(): Unit = {
    if (0 == currentNumber) {
      return
    }

    val newCenter: Array[Double] = new Array[Double](dimension)
    for (i <- newCenter.indices) {
      var sum = 0.0
      for (point <- points) {
        sum += point(i)
      }
      sum /= currentNumber
      newCenter.update(i, sum)
    }

    for (i <- center.indices) {
      center.update(i,
        (center(i) * previousNumber * decayFactor + newCenter(i) * currentNumber)
          / (previousNumber + currentNumber))
    }
  }

  private[streamingkmeans] def getDistance(point: List[Double]): Double = {
    var distance = 0.0
    for (i <- 0 until dimension) {
      distance += ((point(i) - center(i)) * (point(i) - center(i)))
    }
    Math.sqrt(distance)
  }

  override def onStart(startTime: StartTime): Unit = {
    initCenter()
  }

  override def onNext(msg: Message): Unit = {
    if (null == msg) {
      return
    }

    val message = msg.msg.asInstanceOf[ClusterMessage]

    message match {
      case InputMessage(point) =>
        if (isBegin) {
          isBegin = false
          output(new Message((taskContext.taskId.index, getDistance(point), point)))
        } else {
          dataQueue.put(point)
        }
      case ResultMessage(taskId, point, doCluster) =>
        if (taskContext.taskId.index == taskId) {
          points += point
          currentNumber += 1
        }
        if (doCluster) {
          updateCenter()
          LOG.info(s"task ${taskContext.taskId.index}, center ${center.mkString(",")}")
          points.clear()
          previousNumber += currentNumber
          currentNumber = 0
        }
        val newPoint = dataQueue.take()
        output(new Message((taskContext.taskId.index, getDistance(newPoint), newPoint)))
    }
  }

  override def onStop(): Unit = super.onStop()
}
