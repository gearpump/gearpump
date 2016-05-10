/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.experiments.storm.util;

import backtype.storm.utils.TimeCacheMap;

/**
 * Wrapper class to suppress "deprecation" warning, as scala doesn't support the suppression.
 */
@SuppressWarnings("deprecation")
public class TimeCacheMapWrapper<K, V> extends TimeCacheMap<K, V> {

  public TimeCacheMapWrapper (int expirationSecs, Callback<K, V> callback) {
    super(expirationSecs, new ExpiredCallback<K, V>() {

      @Override
      public void expire(K key, V val) {
        callback.expire(key, val);
      }
    });
  }

  public static interface Callback<K, V> {
    public void expire(K key, V val);
  }
}