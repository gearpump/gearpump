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
import com.gs.collections.api.block.procedure.{Procedure, Procedure2}
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
  private val groupedWindowInputs = new UnifiedMap[GROUP, TreeSortedMap[Window, FastList[IN]]]
  private val groupedFnRunners = new UnifiedMap[GROUP, FunctionRunner[IN, OUT]]

  override def process(message: Message): Unit = {
    val input = message.msg.asInstanceOf[IN]
    val (group, windows) = groupBy.groupBy(message)
    if (!groupedWindowInputs.containsKey(group)) {
      groupedWindowInputs.put(group, new TreeSortedMap[Window, FastList[IN]]())
    }
    val windowInputs = groupedWindowInputs.get(group)
    windows.foreach { win =>
      if (windowFn.isNonMerging) {
        if (!windowInputs.containsKey(win)) {
          val inputs = new FastList[IN](1)
          windowInputs.put(win, inputs)
        }
        windowInputs.get(win).add(input)
      } else {
        merge(windowInputs, win, input)
      }
    }

    if (!groupedFnRunners.containsKey(group)) {
      val fn = userConfig.getValue[FunctionRunner[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get
      fn.setup()
      groupedFnRunners.put(group, fn)
    }

    def merge(windowInputs: TreeSortedMap[Window, FastList[IN]], win: Window, input: IN): Unit = {
      val intersected = windowInputs.keySet.select(new Predicate[Window] {
        override def accept(each: Window): Boolean = {
          win.intersects(each)
        }
      })
      var mergedWin = win
      val mergedInputs = FastList.newListWith(input)
      intersected.forEach(new Procedure[Window] {
        override def value(each: Window): Unit = {
          mergedWin = mergedWin.span(each)
          mergedInputs.addAll(windowInputs.remove(each))
        }
      })
      windowInputs.put(mergedWin, mergedInputs)
    }

  }

  override def trigger(time: Instant): Unit = {
    groupedWindowInputs.forEachKeyValue(new Procedure2[GROUP, TreeSortedMap[Window, FastList[IN]]] {
      override def value(group: GROUP, windowInputs: TreeSortedMap[Window, FastList[IN]]): Unit = {
        onTrigger(group, windowInputs)
      }
    })

    @annotation.tailrec
    def onTrigger(group: GROUP, windowInputs: TreeSortedMap[Window, FastList[IN]]): Unit = {
      if (windowInputs.notEmpty()) {
        val firstWin = windowInputs.firstKey
        if (!time.isBefore(firstWin.endTime)) {
          val inputs = windowInputs.remove(firstWin)
          if (groupedFnRunners.containsKey(group)) {
            val reduceFn = AndThen(groupedFnRunners.get(group),
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
            onTrigger(group, windowInputs)
          } else {
            throw new RuntimeException(s"FunctionRunner not found for group $group")
          }
        }
      }
    }

    def emitResult(result: OUT, time: Instant): Unit = {
      taskContext.output(Message(result, time))
    }
  }
}
