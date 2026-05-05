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
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.gearpump.translators.utils.NoOpStepContext;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/** Executes a Beam {@link DoFn} inside a low-level Gearpump task. */
@SuppressWarnings("unchecked")
public class BeamParDoTask<InputT, OutputT> extends Task {

  public static final String PAR_DO_SPEC = "beam.gearpump.par-do-spec";

  private final TaskContext taskContext;
  private final BeamParDoFnSpec<InputT, OutputT> spec;

  private transient DoFnRunner<InputT, OutputT> runner;
  private transient DoFnInvoker<InputT, OutputT> invoker;
  private transient boolean bundleFinished = false;
  private transient boolean tornDown = false;

  public BeamParDoTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
    this.spec = BeamUserConfig.getValue(userConfig, PAR_DO_SPEC, taskContext.system());
  }

  @Override
  public void onStart(Instant startTime) {
    TaskOutputManager outputManager =
        new TaskOutputManager(taskContext, spec.getMainOutputTag(), spec.getSideOutputTags());
    SideInputHandler sideInputReader =
        new SideInputHandler(
            Collections.emptyList(), InMemoryStateInternals.<Void>forKey(null));
    invoker = DoFnInvokers.invokerFor((DoFn<InputT, OutputT>) spec.getDoFn());
    invoker.invokeSetup();
    runner =
        DoFnRunners.simpleRunner(
            spec.getPipelineOptions(),
            spec.getDoFn(),
            sideInputReader,
            outputManager,
            spec.getMainOutputTag(),
            spec.getSideOutputTags(),
            new NoOpStepContext(),
            null,
            spec.getOutputCoders(),
            spec.getWindowingStrategy(),
            spec.getDoFnSchemaInformation());
    runner.startBundle();
  }

  @Override
  public void onNext(Message message) {
    runner.processElement((WindowedValue<InputT>) message.value());
  }

  @Override
  public void onWatermarkProgress(Instant watermark) {
    if (!bundleFinished && Watermark.MAX().equals(watermark)) {
      finishBundle();
      teardownFn();
    }
    taskContext.updateWatermark(watermark);
  }

  @Override
  public void onStop() {
    if (!bundleFinished) {
      finishBundle();
    }
    teardownFn();
  }

  private void finishBundle() {
    runner.finishBundle();
    bundleFinished = true;
  }

  private void teardownFn() {
    if (!tornDown && invoker != null) {
      invoker.invokeTeardown();
      tornDown = true;
    }
  }

  private static final class TaskOutputManager implements DoFnRunners.OutputManager {

    private final TaskContext taskContext;
    private final Set<TupleTag<?>> outputTags = new HashSet<>();

    private TaskOutputManager(
        TaskContext taskContext, TupleTag<?> mainOutputTag, Iterable<TupleTag<?>> sideOutputTags) {
      this.taskContext = taskContext;
      outputTags.add(mainOutputTag);
      for (TupleTag<?> sideOutputTag : sideOutputTags) {
        outputTags.add(sideOutputTag);
      }
    }

    @Override
    public <T> void output(TupleTag<T> outputTag, WindowedValue<T> output) {
      if (outputTags.contains(outputTag)) {
        taskContext.output(
            new DefaultMessage(
                new TaggedOutputValue(outputTag.getId(), output),
                TranslatorUtils.windowedValueTimestamp(output)));
      }
    }
  }
}
