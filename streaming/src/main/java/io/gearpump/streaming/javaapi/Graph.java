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

import io.gearpump.streaming.Processor;
import io.gearpump.streaming.partitioner.Partitioner;
import io.gearpump.streaming.task.Task;

/**
 * Java version of Graph
 *
 * See {@link io.gearpump.util.Graph}
 */
public class Graph extends io.gearpump.util.Graph<Processor<? extends Task>, Partitioner> {

  public Graph() {
    super(null, null);
  }
}