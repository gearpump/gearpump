/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.dsl.window.impl

import java.time.Instant

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.gs.collections.api.block.procedure.Procedure
import org.apache.gearpump.gs.collections.impl.list.mutable.FastList
import org.apache.gearpump.gs.collections.impl.map.mutable.UnifiedMap
import org.apache.gearpump.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.plan.functions.{EmitFunction, SingleInputFunction}
import org.apache.gearpump.streaming.dsl.window.api.Discarding
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

trait WindowRunner {

  def process(message: Message): Unit

  def trigger(time: Instant): Unit

}

object DefaultWindowRunner {

  private val LOG: Logger = LogUtil.getLogger(classOf[DefaultWindowRunner[_, _, _]])

  case class WindowGroup[GROUP](bucket: Bucket, group: GROUP)
    extends Comparable[WindowGroup[GROUP]] {
    override def compareTo(o: WindowGroup[GROUP]): Int = {
      val ret = bucket.compareTo(o.bucket)
      if (ret != 0) {
        ret
      } else if (group.equals(o.group)) {
        0
      } else {
        -1
      }
    }
  }
}

class DefaultWindowRunner[IN, GROUP, OUT](
    taskContext: TaskContext, userConfig: UserConfig,
    groupBy: GroupAlsoByWindow[IN, GROUP])(implicit system: ActorSystem)
  extends WindowRunner {
  import org.apache.gearpump.streaming.dsl.window.impl.DefaultWindowRunner._

  private val windowGroups = new TreeSortedMap[WindowGroup[GROUP], FastList[IN]]
  private val groupFns = new UnifiedMap[GROUP, SingleInputFunction[IN, OUT]]


  override def process(message: Message): Unit = {
    val (group, buckets) = groupBy.groupBy(message)
    buckets.foreach { bucket =>
      val wg = WindowGroup(bucket, group)
      val inputs = windowGroups.getOrDefault(wg, new FastList[IN](1))
      inputs.add(message.msg.asInstanceOf[IN])
      windowGroups.put(wg, inputs)
    }
    groupFns.putIfAbsent(group,
      userConfig.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get)
  }

  override def trigger(time: Instant): Unit = {
    onTrigger()

    @annotation.tailrec
    def onTrigger(): Unit = {
      if (windowGroups.notEmpty()) {
        val first = windowGroups.firstKey
        if (!time.isBefore(first.bucket.endTime)) {
          val inputs = windowGroups.remove(first)
          val reduceFn = groupFns.get(first.group)
            .andThen[Unit](new EmitFunction[OUT](emitResult(_, time)))
          inputs.forEach(new Procedure[IN] {
            override def value(t: IN): Unit = {
              reduceFn.process(t)
            }
          })
          reduceFn.finish()
          if (groupBy.window.accumulationMode == Discarding) {
            reduceFn.clearState()
          }
          onTrigger()
        }
      }
    }

    def emitResult(result: OUT, time: Instant): Unit = {
      taskContext.output(Message(result, time.toEpochMilli))
    }
  }
}
