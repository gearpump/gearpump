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
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;

/** Applies Beam {@code Combine.GroupedValues} to each grouped key/window record. */
@SuppressWarnings("unchecked")
public class BeamCombineGroupedValuesTask<K, InputT, AccumT, OutputT> extends Task {

  public static final String COMBINE_GROUPED_VALUES_SPEC =
      "beam.gearpump.combine-grouped-values-spec";

  private final TaskContext taskContext;
  private final BeamCombineGroupedValuesSpec<K, InputT, OutputT> spec;
  private final GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner;
  private final NullSideInputReader sideInputReader = NullSideInputReader.empty();

  public BeamCombineGroupedValuesTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
    this.spec =
        BeamUserConfig.getValue(
            userConfig, COMBINE_GROUPED_VALUES_SPEC, taskContext.system());
    this.combineFnRunner =
        (GlobalCombineFnRunner<InputT, AccumT, OutputT>)
            GlobalCombineFnRunners.create(
                (org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn<
                        InputT, AccumT, OutputT>)
                    spec.getCombineFn());
  }

  @Override
  public void onNext(Message message) {
    WindowedValue<KV<K, Iterable<InputT>>> windowedValue =
        (WindowedValue<KV<K, Iterable<InputT>>>) message.value();
    KV<K, Iterable<InputT>> groupedValues = windowedValue.getValue();
    AccumT accumulator =
        combineFnRunner.createAccumulator(
            spec.getPipelineOptions(), sideInputReader, windowedValue.getWindows());
    for (InputT value : groupedValues.getValue()) {
      accumulator =
          combineFnRunner.addInput(
              accumulator,
              value,
              spec.getPipelineOptions(),
              sideInputReader,
              windowedValue.getWindows());
    }
    accumulator =
        combineFnRunner.compact(
            accumulator,
            spec.getPipelineOptions(),
            sideInputReader,
            windowedValue.getWindows());
    OutputT output =
        combineFnRunner.extractOutput(
            accumulator,
            spec.getPipelineOptions(),
            sideInputReader,
            windowedValue.getWindows());
    WindowedValue<KV<K, OutputT>> outputValue =
        WindowedValues.withValue(windowedValue, KV.of(groupedValues.getKey(), output));
    taskContext.output(new DefaultMessage(outputValue, message.timestamp()));
  }

  @Override
  public void onWatermarkProgress(Instant watermark) {
    taskContext.updateWatermark(watermark);
  }
}
