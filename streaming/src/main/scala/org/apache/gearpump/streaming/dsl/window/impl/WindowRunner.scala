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
import com.gs.collections.api.block.procedure.Procedure
import com.gs.collections.impl.list.mutable.FastList
import com.gs.collections.impl.map.mutable.UnifiedMap
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import com.gs.collections.impl.set.mutable.UnifiedSet
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.plan.functions.{AndThen, Emit, SingleInputFunction}
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
}

class DefaultWindowRunner[IN, GROUP, OUT](
    taskContext: TaskContext, userConfig: UserConfig,
    groupBy: GroupAlsoByWindow[IN, GROUP])(implicit system: ActorSystem)
  extends WindowRunner {
  import org.apache.gearpump.streaming.dsl.window.impl.DefaultWindowRunner._

  private val windows = new TreeSortedMap[Bucket, UnifiedSet[WindowGroup[GROUP]]]
  private val windowGroups = new UnifiedMap[WindowGroup[GROUP], FastList[IN]]
  private val groupFns = new UnifiedMap[GROUP, SingleInputFunction[IN, OUT]]

  override def process(message: Message): Unit = {
    val (group, buckets) = groupBy.groupBy(message)
    buckets.foreach { bucket =>
      val wg = WindowGroup(bucket, group)
      val wgs = windows.getOrDefault(bucket, new UnifiedSet[WindowGroup[GROUP]](1))
      wgs.add(wg)
      windows.put(bucket, wgs)

      val inputs = windowGroups.getOrDefault(wg, new FastList[IN](1))
      inputs.add(message.msg.asInstanceOf[IN])
      windowGroups.put(wg, inputs)
    }
    if (!groupFns.containsKey(group)) {
      val fn = userConfig.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get
      fn.setup()
      groupFns.put(group, fn)
    }
  }

  override def trigger(time: Instant): Unit = {
    onTrigger()

    @annotation.tailrec
    def onTrigger(): Unit = {
      if (windows.notEmpty()) {
        val first = windows.firstKey
        if (!time.isBefore(first.endTime)) {
          val wgs = windows.remove(first)
          wgs.forEach(new Procedure[WindowGroup[GROUP]] {
            override def value(each: WindowGroup[GROUP]): Unit = {
              val inputs = windowGroups.remove(each)
              val reduceFn = AndThen(groupFns.get(each.group), new Emit[OUT](emitResult(_, time)))
              inputs.forEach(new Procedure[IN] {
                override def value(t: IN): Unit = {
                  // .toList forces eager evaluation
                  reduceFn.process(t).toList
                }
              })
              // .toList forces eager evaluation
              reduceFn.finish().toList
              if (groupBy.window.accumulationMode == Discarding) {
                reduceFn.teardown()
              }
            }
          })

          onTrigger()
        }
      }
    }

    def emitResult(result: OUT, time: Instant): Unit = {
      taskContext.output(Message(result, time.toEpochMilli))
    }
  }
}
