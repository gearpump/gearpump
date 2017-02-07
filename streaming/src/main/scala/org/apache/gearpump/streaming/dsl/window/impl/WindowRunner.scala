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
import com.gs.collections.api.block.predicate.Predicate
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import com.gs.collections.api.block.procedure.Procedure
import com.gs.collections.impl.list.mutable.FastList
import com.gs.collections.impl.map.mutable.UnifiedMap
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.plan.functions.{AndThen, Emit, FunctionRunner}
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
}

class DefaultWindowRunner[IN, GROUP, OUT](
    taskContext: TaskContext, userConfig: UserConfig,
    groupBy: GroupAlsoByWindow[IN, GROUP])(implicit system: ActorSystem)
  extends WindowRunner {

  private val windowFn = groupBy.window.windowFn
  private val groupedInputs = new TreeSortedMap[WindowAndGroup[GROUP], FastList[IN]]
  private val groupedFnRunners = new UnifiedMap[GROUP, FunctionRunner[IN, OUT]]

  override def process(message: Message): Unit = {
    val input = message.msg.asInstanceOf[IN]
    val wgs = groupBy.groupBy(message)
    wgs.foreach { wg =>
      if (windowFn.isNonMerging) {
        if (!groupedInputs.containsKey(wg)) {
          val inputs = new FastList[IN](1)
          groupedInputs.put(wg, inputs)
        }
        groupedInputs.get(wg).add(input)
      } else {
        merge(wg, input)
      }

      if (!groupedFnRunners.containsKey(wg.group)) {
        val fn = userConfig.getValue[FunctionRunner[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get
        fn.setup()
        groupedFnRunners.put(wg.group, fn)
      }
    }

    def merge(wg: WindowAndGroup[GROUP], input: IN): Unit = {
      val intersected = groupedInputs.keySet.select(new Predicate[WindowAndGroup[GROUP]] {
        override def accept(each: WindowAndGroup[GROUP]): Boolean = {
          wg.intersects(each)
        }
      })
      var mergedWin = wg.window
      val mergedInputs = FastList.newListWith(input)
      intersected.forEach(new Procedure[WindowAndGroup[GROUP]] {
        override def value(each: WindowAndGroup[GROUP]): Unit = {
          mergedWin = mergedWin.span(each.window)
          mergedInputs.addAll(groupedInputs.remove(each))
        }
      })
      groupedInputs.put(WindowAndGroup(mergedWin, wg.group), mergedInputs)
    }

  }

  override def trigger(time: Instant): Unit = {
    onTrigger()

    @annotation.tailrec
    def onTrigger(): Unit = {
      if (groupedInputs.notEmpty()) {
        val first = groupedInputs.firstKey
        if (!time.isBefore(first.window.endTime)) {
          val inputs = groupedInputs.remove(first)
          if (groupedFnRunners.containsKey(first.group)) {
            val reduceFn = AndThen(groupedFnRunners.get(first.group),
              new Emit[OUT](output => emitResult(output, time)))
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
            onTrigger()
          } else {
            throw new RuntimeException(s"FunctionRunner not found for group ${first.group}")
          }
        }
      }
    }

    def emitResult(result: OUT, time: Instant): Unit = {
      taskContext.output(Message(result, time.toEpochMilli))
    }
  }
}
