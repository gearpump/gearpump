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

import io.gearpump.util.LogUtil
import io.gearpump.{TimeStamp, Message}
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.TaskContext
import org.slf4j.Logger

import scala.util.Random

object RandomRBFSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[RandomRBFSource])
}

/**
 * RandomRBFGenerator generates via radial basis function.
 * Reference by https://github.com/huawei-noah/streamDM
 */
class RandomRBFSource(k: Int, dimension: Int) extends DataSource {

  class Centroid(center: Array[Double], classLab: Int, stdev: Double) {
    val centre = center
    val classLabel = classLab
    val stdDev = stdev
  }

  val centroids = new Array[Centroid](k)
  val centroidWeights = new Array[Double](centroids.length)
  val instanceRandom: Random = new Random()

  def generateCentroids(): Unit = {
    val modelRand: Random = new Random()

    for (i <- centroids.indices) {
      val randCentre: Array[Double] = Array.fill[Double](dimension)(modelRand.nextDouble())
      centroids.update(i, new Centroid(randCentre, modelRand.nextInt(k), modelRand.nextDouble()))
      centroidWeights.update(i, modelRand.nextDouble())
    }
  }

  /**
   * choose an index of the weight array randomly.
   * @param weights Weight Array
   * @param random Random value generator
   * @return an index of the weight array
   */
  private def chooseRandomIndexBasedOnWeights(weights: Array[Double], random: Random): Int = {
    val probSum = weights.sum
    val ran = random.nextDouble() * probSum
    var index: Int = 0
    var sum: Double = 0.0
    while ((sum <= ran) && (index < weights.length)) {
      sum += weights(index)
      index += 1
    }
    index - 1
  }

  def getPoint: List[Double] = {
    val index = chooseRandomIndexBasedOnWeights(centroidWeights, instanceRandom)
    val centroid: Centroid = centroids(index)

    val initFeatureVals:Array[Double] = Array.fill[Double](dimension)(
        instanceRandom.nextDouble() * 2.0 - 1.0)
    val magnitude = Math.sqrt(initFeatureVals.foldLeft(0.0){(a,x) => a + x * x})

    val desiredMag = instanceRandom.nextGaussian() * centroid.stdDev
    val scale = desiredMag / magnitude

    val featureVals = centroid.centre zip initFeatureVals map {case (a,b) => a + b * scale}
    featureVals.toList
  }

  /**
   * open connection to data source
   * invoked in onStart() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
    generateCentroids()
  }

  /**
   * close connection to data source.
   * invoked in onStop() method of [[io.gearpump.streaming.source.DataSourceTask]]
   */
  override def close(): Unit = {}

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param batchSize max number of messages to read
   * @return a list of messages wrapped in [[io.gearpump.Message]]
   */
  override def read(batchSize: Int): List[Message] = {
    List.fill(batchSize)(new Message(new InputMessage(getPoint)))
  }
}
