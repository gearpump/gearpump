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

package io.gearpump.streaming.javaapi;

import akka.actor.ActorRef;
import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.TaskContext;

import java.time.Instant;

/**
 * Java version of Task.
 *
 * See {@link io.gearpump.streaming.task.Task}
 */
public class Task extends io.gearpump.streaming.task.Task {
  protected TaskContext context;
  protected UserConfig userConf;

  public Task(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
    this.context = taskContext;
    this.userConf = userConf;
  }

  @Override
  final public ActorRef self() {
    return context.self();
  }

  @Override
  public void onStart(Instant startTime) {
  }

  @Override
  public void onNext(Message msg) {
  }

  @Override
  public void onStop() {
  }
}