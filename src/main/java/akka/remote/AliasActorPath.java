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

package akka.remote;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import scala.collection.*;

import java.lang.Iterable;

/**
 * Change toSerializationFormat to serialize to a shortten string representation
 * Hack this in java since ActorPath is a sealed trait, cannot do extends it in scala directly
 */
public class AliasActorPath implements ActorPath {
  private ActorPath path;
  private String alias;

  public AliasActorPath(String alias, ActorPath path) {
    this.path = path;
    this.alias = alias;
  }

  @Override
  public Address address() {
    return path.address();
  }

  @Override
  public String name() {
    return path.name();
  }

  @Override
  public ActorPath parent() {
    return path.parent();
  }

  @Override
  public ActorPath $div(String child) {
    return path.$div(child);
  }

  @Override
  public ActorPath child(String child) {
    return path.child(child);
  }

  @Override
  public ActorPath $div(scala.collection.Iterable<String> child) {
    return path.$div(child);
  }

  @Override
  public Iterable<String> getElements() {
    return path.getElements();
  }

  @Override
  public ActorPath descendant(Iterable<String> names) {
    return path.descendant(names);
  }

  @Override
  public scala.collection.immutable.Iterable<String> elements() {
    return path.elements();
  }

  @Override
  public RootActorPath root() {
    return path.root();
  }

  @Override
  public String toStringWithoutAddress() {
    return path.toStringWithoutAddress();
  }

  @Override
  public String toStringWithAddress(Address address) {
    return path.toStringWithAddress(address);
  }

  // TODO: modifify this so that we can hack the wire format
  @Override
  public String toSerializationFormat() {
    return alias;
  }

  //TODO: Currently this api is not used, but we cannot gurantee that it won't be used in future.
  @Override
  public String toSerializationFormatWithAddress(Address address) {
    return path.toSerializationFormatWithAddress(address);
  }

  @Override
  public int uid() {
    return path.uid();
  }

  @Override
  public ActorPath withUid(int uid) {
    return path.withUid(uid);
  }

  @Override
  public int compareTo(ActorPath o) {
    return path.compareTo(o);
  }
}