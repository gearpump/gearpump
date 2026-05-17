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
import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashSet;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;

/** Applies Beam window assignment to incoming {@link WindowedValue} elements. */
@SuppressWarnings("unchecked")
public class BeamAssignWindowsTask<T> extends Task {

  public static final String ASSIGN_WINDOWS_SPEC = "beam.gearpump.assign-windows-spec";

  private final TaskContext taskContext;
  private final BeamAssignWindowsSpec<T> spec;

  public BeamAssignWindowsTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
    this.spec =
        BeamUserConfig.getValue(
            userConfig, ASSIGN_WINDOWS_SPEC, taskContext.system());
  }

  @Override
  public void onNext(Message message) {
    WindowedValue<T> windowedValue = (WindowedValue<T>) message.value();
    emitAssignedWindows(windowedValue);
  }

  @Override
  public void onWatermarkProgress(Instant watermark) {
    taskContext.updateWatermark(watermark);
  }

  private void emitAssignedWindows(WindowedValue<T> windowedValue) {
    try {
      Collection<BoundedWindow> assignedWindows = new LinkedHashSet<>();
      for (WindowedValue<T> explodedWindow : windowedValue.explodeWindows()) {
        assignedWindows.addAll(assignWindowsFor(explodedWindow));
      }
      WindowedValue<T> assignedValue =
          WindowedValues.of(
              windowedValue.getValue(),
              windowedValue.getTimestamp(),
              assignedWindows,
              windowedValue.getPaneInfo(),
              windowedValue.getRecordId(),
              windowedValue.getRecordOffset(),
              windowedValue.causedByDrain());
      taskContext.output(
          new DefaultMessage(
              assignedValue,
              TranslatorUtils.windowedValueTimestamp(assignedValue)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to assign Beam windows", e);
    }
  }

  private Collection<BoundedWindow> assignWindowsFor(WindowedValue<T> windowedValue)
      throws Exception {
    WindowFn<T, BoundedWindow> windowFn = (WindowFn<T, BoundedWindow>) spec.getWindowFn();
    return windowFn.assignWindows(
        windowFn.new AssignContext() {
          @Override
          public T element() {
            return windowedValue.getValue();
          }

          @Override
          public org.joda.time.Instant timestamp() {
            return windowedValue.getTimestamp();
          }

          @Override
          public BoundedWindow window() {
            return (BoundedWindow) windowedValue.getWindows().iterator().next();
          }
        });
  }
}
