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
import org.apache.beam.runners.gearpump.runtime.BeamCombineGroupedValuesSpec;
import org.apache.beam.runners.gearpump.runtime.BeamCombineGroupedValuesTask;
import org.apache.beam.runners.gearpump.runtime.BeamUserConfig;
import org.apache.beam.sdk.transforms.Combine;

/** Translates Beam {@link Combine.GroupedValues} into a low-level keyed combine task. */
public class CombineGroupedValuesTranslator<K, InputT, OutputT>
    implements TransformTranslator<Combine.GroupedValues<K, InputT, OutputT>> {

  @Override
  public void translate(
      Combine.GroupedValues<K, InputT, OutputT> transform, TranslationContext context) {
    if (!transform.getSideInputs().isEmpty()) {
      throw new UnsupportedOperationException(
          "The low-level Gearpump Beam runner does not support combine side inputs yet.");
    }

    BeamCombineGroupedValuesSpec<K, InputT, OutputT> spec =
        new BeamCombineGroupedValuesSpec<>(context.getPipelineOptions(), transform.getFn());
    UserConfig userConfig =
        BeamUserConfig.withValue(
            UserConfig.empty(),
            BeamCombineGroupedValuesTask.COMBINE_GROUPED_VALUES_SPEC,
            spec,
            context.getActorSystem());
    Processor<BeamCombineGroupedValuesTask> combine =
        context.addProcessor(
            BeamCombineGroupedValuesTask.class,
            userConfig,
            transform.getName());
    context.connect(context.getInput(), new CoLocationPartitioner(), combine);
    context.setOutputProcessor(context.getOutput(), combine);
  }
}
