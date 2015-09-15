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

package io.gearpump.streaming.examples.wordcountjava;

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.javaapi.Task;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.TaskContext;

public class Split extends Task {

  public static String TEXT = "This is a good start for java! bingo! bingo! ";

  public Split(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
  }

  private Long now() {
    return System.currentTimeMillis();
  }

  @Override
  public void onStart(StartTime startTime) {
    self().tell(new Message("start", now()), self());
  }

  @Override
  public void onNext(Message msg) {

    // Split the TEXT to words
    String[] words = TEXT.split(" ");
    for (int i = 0; i < words.length; i++) {
      context.output(new Message(words[i], now()));
    }
    self().tell(new Message("next", now()), self());
  }
}