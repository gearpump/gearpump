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
import org.apache.beam.sdk.values.WindowedValue;

/** Filters ParDo output by Beam output tag and unwraps the contained value. */
public class BeamTaggedOutputTask extends Task {

  public static final String OUTPUT_TAG = "beam.gearpump.output-tag";

  private final TaskContext taskContext;
  private final String outputTag;

  public BeamTaggedOutputTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
    this.outputTag = (String) userConfig.getString(OUTPUT_TAG).get();
  }

  @Override
  public void onNext(Message message) {
    TaggedOutputValue taggedOutput = (TaggedOutputValue) message.value();
    if (outputTag.equals(taggedOutput.getOutputTag())) {
      WindowedValue<?> windowedValue = taggedOutput.getValue();
      taskContext.output(new DefaultMessage(windowedValue, message.timestamp()));
    }
  }
}
