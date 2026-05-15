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
package org.apache.beam.runners.gearpump.translators;

import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.partitioner.CoLocationPartitioner;
import org.apache.beam.runners.gearpump.runtime.BeamAssignWindowsSpec;
import org.apache.beam.runners.gearpump.runtime.BeamAssignWindowsTask;
import org.apache.beam.runners.gearpump.runtime.BeamUserConfig;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

/** Translates Beam {@link Window.Assign} into a low-level Gearpump window-assignment task. */
@SuppressWarnings("unchecked")
public class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {

  @Override
  public void translate(Window.Assign<T> transform, TranslationContext context) {
    PCollection<T> input = (PCollection<T>) context.getInput();
    BeamAssignWindowsSpec<T> spec =
        new BeamAssignWindowsSpec<>(transform.getWindowFn(), input.getWindowingStrategy());
    UserConfig userConfig =
        BeamUserConfig.withValue(
            UserConfig.empty(),
            BeamAssignWindowsTask.ASSIGN_WINDOWS_SPEC,
            spec,
            context.getActorSystem());
    Processor<BeamAssignWindowsTask> assignWindows =
        context.addProcessor(BeamAssignWindowsTask.class, userConfig, transform.getName());
    context.connect(context.getInput(), new CoLocationPartitioner(), assignWindows);
    context.setOutputProcessor(context.getOutput(), assignWindows);
  }
}
