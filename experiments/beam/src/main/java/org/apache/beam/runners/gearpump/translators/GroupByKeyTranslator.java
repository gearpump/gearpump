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
import org.apache.beam.runners.gearpump.runtime.BeamGroupByKeySpec;
import org.apache.beam.runners.gearpump.runtime.BeamGroupByKeyTask;
import org.apache.beam.runners.gearpump.runtime.BeamKeyPartitioner;
import org.apache.beam.runners.gearpump.runtime.BeamUserConfig;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Translates Beam {@link GroupByKey} into a low-level Gearpump in-memory grouping task. */
@SuppressWarnings("unchecked")
public class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {

  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) context.getInput();
    if (!input.getWindowingStrategy().getWindowFn().isNonMerging()) {
      throw new UnsupportedOperationException(
          "The low-level Gearpump Beam runner currently supports GroupByKey only for "
              + "non-merging windows.");
    }

    Coder<K> keyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
    Coder<? extends BoundedWindow> windowCoder =
        input.getWindowingStrategy().getWindowFn().windowCoder();
    TimestampCombiner timestampCombiner = input.getWindowingStrategy().getTimestampCombiner();
    BeamGroupByKeySpec<K> spec =
        new BeamGroupByKeySpec<>(keyCoder, windowCoder, timestampCombiner);
    UserConfig userConfig =
        BeamUserConfig.withValue(
            UserConfig.empty(),
            BeamGroupByKeyTask.GROUP_BY_KEY_SPEC,
            spec,
            context.getActorSystem());
    Processor<BeamGroupByKeyTask> groupByKey =
        context.addProcessor(BeamGroupByKeyTask.class, userConfig, transform.getName());
    context.connect(context.getInput(), new BeamKeyPartitioner<>(keyCoder), groupByKey);
    context.setOutputProcessor(context.getOutput(), groupByKey);
  }
}
