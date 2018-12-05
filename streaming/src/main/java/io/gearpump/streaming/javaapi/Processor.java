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

import akka.actor.ActorSystem;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.sink.DataSink;
import io.gearpump.streaming.sink.DataSinkProcessor;
import io.gearpump.streaming.sink.DataSinkTask;
import io.gearpump.streaming.source.DataSource;
import io.gearpump.streaming.source.DataSourceProcessor;
import io.gearpump.streaming.source.DataSourceTask;

/**
 * Java version of Processor
 *
 * See {@link io.gearpump.streaming.Processor}
 */
public class Processor<T extends io.gearpump.streaming.task.Task> implements io.gearpump.streaming.Processor<T> {
  private Class<T> _taskClass;
  private int _parallelism = 1;
  private String _description = "";
  private UserConfig _userConf = UserConfig.empty();

  public Processor(Class<T> taskClass) {
    this._taskClass = taskClass;
  }

  public Processor(Class<T> taskClass, int parallelism) {
    this._taskClass = taskClass;
    this._parallelism = parallelism;
  }

  /**
   * Creates a Sink Processor
   *
   * @param dataSink    the data sink itself
   * @param parallelism the parallelism of this processor
   * @param description the description for this processor
   * @param taskConf    the configuration for this processor
   * @param system      actor system
   * @return the new created sink processor
   */
  public static Processor<DataSinkTask> sink(DataSink dataSink, int parallelism, String description, UserConfig taskConf, ActorSystem system) {
    io.gearpump.streaming.Processor<DataSinkTask> p = DataSinkProcessor.apply(dataSink, parallelism, description, taskConf, system);
    return new Processor(p);
  }

  /**
   * Creates a Source Processor
   *
   * @param source      the data source itself
   * @param parallelism the parallelism of this processor
   * @param description the description of this processor
   * @param taskConf    the configuration of this processor
   * @param system      actor system
   * @return the new created source processor
   */
  public static Processor<DataSourceTask> source(DataSource source, int parallelism, String description, UserConfig taskConf, ActorSystem system) {
    io.gearpump.streaming.Processor<DataSourceTask<Object, Object>> p =
        DataSourceProcessor.apply(source, parallelism, description, taskConf, system);
    return new Processor(p);
  }

  public Processor(io.gearpump.streaming.Processor<T> processor) {
    this._taskClass = (Class) (processor.taskClass());
    this._parallelism = processor.parallelism();
    this._description = processor.description();
    this._userConf = processor.taskConf();
  }

  /**
   * Creates a general processor with user specified task logic.
   *
   * @param taskClass    task implementation class of this processor (shall be a derived class from {@link Task}
   * @param parallelism, how many initial tasks you want to use
   * @param description, some text to describe this processor
   * @param taskConf,    Processor specific configuration
   */
  public Processor(Class<T> taskClass, int parallelism, String description, UserConfig taskConf) {
    this._taskClass = taskClass;
    this._parallelism = parallelism;
    this._description = description;
    this._userConf = taskConf;
  }

  public Processor<T> withParallelism(int parallel) {
    return new Processor<T>(_taskClass, parallel, _description, _userConf);
  }

  public Processor<T> withDescription(String desc) {
    return new Processor<T>(_taskClass, _parallelism, desc, _userConf);
  }

  public Processor<T> withConfig(UserConfig conf) {
    return new Processor<T>(_taskClass, _parallelism, _description, conf);
  }

  @Override
  public int parallelism() {
    return _parallelism;
  }

  @Override
  public UserConfig taskConf() {
    return _userConf;
  }

  @Override
  public String description() {
    return _description;
  }

  @Override
  public Class<? extends io.gearpump.streaming.task.Task> taskClass() {
    return _taskClass;
  }

  /**
   * reference equal
   */
  @Override
  public boolean equals(Object obj) {
    return (this == obj);
  }
}