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

package io.gearpump.streaming.javaapi;

import io.gearpump.cluster.UserConfig;

public class Processor<T extends Task> implements io.gearpump.streaming.Processor<T> {
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
   *
   * @param taskClass Class\<T extends Task\> implementation class of this processor
   * @param parallelism, how many initial tasks you want to use
   * @param description, some text to describe this processor
   * @param taskConf, Processor specific configuration
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
  public Class<? extends Task> taskClass() {
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