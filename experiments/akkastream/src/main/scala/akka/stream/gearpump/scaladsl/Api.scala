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

package akka.stream.gearpump.scaladsl

import akka.stream.{FlowShape, Graph, Attributes}
import akka.stream.gearpump.module.{ProcessorModule, ReduceModule, GroupByModule, DummySink, DummySource, SinkBridgeModule, SinkTaskModule, SourceBridgeModule, SourceTaskModule}
import akka.stream.impl.Stages.Map
import akka.stream.scaladsl.{FlowOps, Flow, Keep, Sink, Source}
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.Task
import org.reactivestreams.{Publisher, Subscriber}


object GearSource{

  /**
   * Construct a Source which accepts out of band input messages.
   *
   *                   [[SourceBridgeModule]] -> Sink
   *                          /
   *                         /
   *                        V
   *                materialize to [[Subscriber]]
   *                                   /|
   *                                  /
   *       upstream [[Publisher]] send out of band message
   *
   */
  def bridge[IN, OUT]: Source[OUT, Subscriber[IN]] = {
    val source = new Source(new DummySource[IN](Attributes.name("dummy"), Source.shape("dummy")))
    val flow = new Flow[IN, OUT, Subscriber[IN]](new SourceBridgeModule[IN, OUT]())
    source.viaMat(flow)(Keep.right)
  }

  /**
   * Construct a Source from Gearpump [[DataSource]].
   *
   *    [[SourceTaskModule]] -> downstream Sink
   *
   */
  def from[OUT](source: DataSource): Source[OUT, Unit] = {
    val taskSource = new Source[OUT, Unit](new SourceTaskModule(source, UserConfig.empty))
    taskSource
  }

  /**
   * Construct a Source from Gearpump [[io.gearpump.streaming.Processor]].
   *
   *    [[ProcessorModule]] -> downstream Sink
   *
   */
  def from[OUT](processor: Class[_ <: Task], conf: UserConfig): Source[OUT, Unit] = {
    val source = new Source(new DummySource[Unit](Attributes.name("dummy"), Source.shape("dummy")))
    val flow = Processor.apply[Unit, OUT](processor, conf)
    source.viaMat(flow)(Keep.right)
  }
}

object GearSink {

  /**
   * Construct a Sink which output messages to a out of band channel.
   *
   *   Souce ->   [[SinkBridgeModule]]
   *                    \
   *                     \|
   *         materialize to [[Publisher]]
   *                              \
   *                               \
   *                                \|
   *       send out of band message to downstream [[Subscriber]]
   *
   */
  def bridge[IN, OUT]: Sink[IN, Publisher[OUT]] = {
    val sink = new Sink(new DummySink[OUT](Attributes.name("dummy"), Sink.shape("dummy")))
    val flow = new Flow[IN, OUT, Publisher[OUT]](new SinkBridgeModule[IN, OUT]())
    flow.to(sink)
  }

  /**
   * Construct a Sink from Gearpump [[DataSink]].
   *
   *    Upstream Source -> [[SinkTaskModule]]
   *
   */
  def to[IN](sink: DataSink): Sink[IN, Unit] = {
    val taskSink = new Sink[IN, Unit](new SinkTaskModule(sink, UserConfig.empty))
    taskSink
  }

  /**
   * Construct a Sink from Gearpump [[io.gearpump.streaming.Processor]].
   *
   *    Upstream Source -> [[ProcessorModule]]
   *
   */
  def to[IN](processor: Class[_ <: Task], conf: UserConfig): Sink[IN, Unit] = {
    val sink = new Sink(new DummySink[Unit](Attributes.name("dummy"), Sink.shape("dummy")))
    val flow = Processor.apply[IN, Unit](processor, conf)
    flow.to(sink)
  }
}

/**
 *
 * GroupBy will divide the main input stream to a set of sub-streams.
 * This is a work-around to bypass the limitation of official API Flow.groupBy
 *
 *
 * For example, to do a word count, we can write code like this:
 *
 * case class KV(key: String, value: String)
 * case class Count(key: String, count: Int)
 *
 * val flow: Flow[KV] = GroupBy[KV](foo).map{ kv =>
 *   Count(kv.key, 1)
 * }.fold(Count(null, 0)){(base, add) =>
 *   Count(add.key, base.count + add.count)
 * }.log("count of current key")
 * .flatten()
 * .to(sink)
 *
 * map, fold will transform data on all sub-streams, If there are 10 groups,
 * then there will be 10 sub-streams, and for each sub-stream, there will be
 * a map and fold.
 *
 * flatten will collect all sub-stream into the main stream,
 *
 * sink will only operate on the main stream.
 *
 */
object GroupBy{
  def apply[T, Group](groupBy: T=>Group): Flow[T, T, Unit] = {
    new Flow[T, T, Unit](new GroupByModule(groupBy))
  }
}

/**
 * Aggregate on the data.
 *
 * val flow = Reduce({(a: Int, b: Int) => a + b})
 *
 *
 */
object Reduce{
  def apply[T](reduce: (T, T) => T): Flow[T, T, Unit] = {
    new Flow[T, T, Unit](new ReduceModule(reduce))
  }
}


/**
 * Create a Flow by providing a Gearpump Processor class and configuration
 *
 *
 */
object Processor{
  def apply[In, Out](processor: Class[_ <: Task], conf: UserConfig): Flow[In, Out, Unit] = {
    new Flow[In, Out, Unit](new ProcessorModule[In, Out, Unit](processor, conf))
  }
}

object Implicits {

  /**
   * Help util to support reduce and groupBy
   */
  implicit class SourceOps[T, Mat](source: Source[T, Mat]) {

    //TODO It is named as groupBy2 to avoid conflict with built-in
    // groupBy. Eventually, we think the built-in groupBy should
    // be replace with this implementation.
    def groupBy2[Group](groupBy: T => Group): Source[T, Mat] = {
      val stage = GroupBy.apply(groupBy)
      source.via[T, Unit](stage)
    }


    def reduce(reduce: (T, T) => T): Source[T, Mat] = {
      val stage = Reduce.apply(reduce)
      source.via[T, Unit](stage)
    }

    def process[R](processor: Class[_ <: Task], conf: UserConfig): Source[R, Mat] = {
      val stage = Processor.apply[T, R](processor, conf)
      source.via(stage)
    }
  }

  /**
   * Help util to support reduce and groupBy
   */
  implicit class FlowOps[IN, OUT, Mat](flow: Flow[IN, OUT, Mat]) {
    def groupBy2[Group](groupBy: OUT => Group): Flow[IN, OUT, Mat] = {
      val stage = GroupBy.apply(groupBy)
      flow.via(stage)
    }

    def reduce(reduce: (OUT, OUT) => OUT): Flow[IN, OUT, Mat] = {
      val stage = Reduce.apply(reduce)
      flow.via(stage)
    }

    def process[R](processor: Class[_ <: Task], conf: UserConfig): Flow[IN, R, Mat] = {
      val stage = Processor.apply[OUT, R](processor, conf)
      flow.via(stage)
    }
  }

  /**
   * Help util to support groupByKey and sum
   */
  implicit class KVSourceOps[K, V, Mat](source: Source[(K, V), Mat]) {

    /**
     * if it is a KV Pair, we can group the KV pair by the key.
     * @return
     */
    def groupByKey: Source[(K, V), Mat] = {
      val stage = GroupBy.apply(getTupleKey[K, V])
      source.via(stage)
    }

    /**
     * do sum on values
     *
     * Before doing this, you need to do groupByKey to group same key together
     * , otherwise, it will do the sum no matter what current key is.
     *
     * @param numeric
     * @return
     */
    def sumOnValue(implicit numeric: Numeric[V]): Source[(K, V), Mat] = {
      val stage = Reduce.apply(sumByValue[K, V](numeric))
      source.via(stage)
    }
  }

  /**
   * Help util to support groupByKey and sum
   */
  implicit class KVFlowOps[K, V, Mat](flow: Flow[(K, V), (K, V), Mat]) {

    /**
     * if it is a KV Pair, we can group the KV pair by the key.
     * @return
     */
    def groupByKey: Flow[(K, V), (K, V), Mat] = {
      val stage = GroupBy.apply(getTupleKey[K, V])
      flow.via(stage)
    }

    /**
     * do sum on values
     *
     * Before doing this, you need to do groupByKey to group same key together
     * , otherwise, it will do the sum no matter what current key is.
     *
     * @param numeric
     * @return
     */
    def sumOnValue(implicit numeric: Numeric[V]): Flow[(K, V), (K, V), Mat] = {
      val stage = Reduce.apply(sumByValue[K, V](numeric))
      flow.via(stage)
    }
  }

  private def getTupleKey[K, V](tuple: Tuple2[K, V]): K = tuple._1

  private def sumByValue[K, V](numeric: Numeric[V]): (Tuple2[K, V], Tuple2[K, V]) => Tuple2[K, V]
    = (tuple1, tuple2) => Tuple2(tuple1._1, numeric.plus(tuple1._2, tuple2._2))
}