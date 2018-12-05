/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.examples.wordcountjava;

import io.gearpump.DefaultMessage;
import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.javaapi.Task;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.TaskContext;

import java.time.Instant;

public class Split extends Task {

  public static String TEXT = "This is a good start for java! bingo! bingo! ";

  public Split(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
  }

  @Override
  public void onStart(Instant startTime) {
    self().tell(new Watermark(Instant.now()), self());
  }

  @Override
  public void onNext(Message msg) {

    // Split the TEXT to words
    String[] words = TEXT.split(" ");
    for (int i = 0; i < words.length; i++) {
      context.output(DefaultMessage.apply(words[i], Instant.now().toEpochMilli()));
    }
    self().tell(new Watermark(Instant.now()), self());
  }
}