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
package org.apache.beam.runners.gearpump.runtime;

import io.gearpump.DefaultMessage;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.pekko.actor.Cancellable;
import scala.Function1;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/** Emits the single Beam impulse element after task startup stabilizes. */
public class BeamImpulseTask extends Task {

  private static final FiniteDuration STARTUP_DELAY = Duration.create(200, TimeUnit.MILLISECONDS);
  private static final FiniteDuration WATERMARK_DELAY = Duration.create(200, TimeUnit.MILLISECONDS);
  private static final Object EMIT_IMPULSE = new Object();
  private static final Object ADVANCE_WATERMARK = new Object();

  private final TaskContext taskContext;
  private transient Cancellable scheduledImpulse;
  private transient Cancellable scheduledWatermark;

  public BeamImpulseTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
  }

  @Override
  public void onStart(Instant startTime) {
    if (taskContext.taskId().index() == 0) {
      scheduledImpulse =
          taskContext.scheduleOnce(
              STARTUP_DELAY,
              new AbstractFunction0<>() {
                @Override
                public BoxedUnit apply() {
                  taskContext.self().tell(EMIT_IMPULSE, taskContext.self());
                  return BoxedUnit.UNIT;
                }
              });
    } else {
      taskContext.updateWatermark(Watermark.MAX());
    }
  }

  @Override
  public void onStop() {
    if (scheduledImpulse != null) {
      scheduledImpulse.cancel();
      scheduledImpulse = null;
    }
    if (scheduledWatermark != null) {
      scheduledWatermark.cancel();
      scheduledWatermark = null;
    }
  }

  @Override
  public PartialFunction<Object, BoxedUnit> receiveUnManagedMessage() {
    return new AbstractPartialFunction<Object, BoxedUnit>() {
      @Override
      public <A1 extends Object, B1> B1 applyOrElse(A1 value, Function1<A1, B1> defaultFn) {
        if (value == EMIT_IMPULSE) {
          emitImpulse();
          scheduleWatermarkAdvance();
          return (B1) BoxedUnit.UNIT;
        }
        if (value == ADVANCE_WATERMARK) {
          advanceWatermark();
          return (B1) BoxedUnit.UNIT;
        }
        return defaultFn.apply(value);
      }

      @Override
      public boolean isDefinedAt(Object value) {
        return value == EMIT_IMPULSE || value == ADVANCE_WATERMARK;
      }
    };
  }

  private void emitImpulse() {
    WindowedValue<byte[]> impulse = WindowedValues.valueInGlobalWindow(new byte[0]);
    taskContext.output(new DefaultMessage(impulse, TranslatorUtils.windowedValueTimestamp(impulse)));
  }

  private void scheduleWatermarkAdvance() {
    scheduledWatermark =
        taskContext.scheduleOnce(
            WATERMARK_DELAY,
            new AbstractFunction0<>() {
              @Override
              public BoxedUnit apply() {
                taskContext.self().tell(ADVANCE_WATERMARK, taskContext.self());
                return BoxedUnit.UNIT;
              }
            });
  }

  private void advanceWatermark() {
    taskContext.updateWatermark(Watermark.MAX());
  }
}
