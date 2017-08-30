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

package org.apache.gearpump.sql.table;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;

public class SampleString {

  public static JavaStream<String> WORDS;

  public static class Stream {
    public static final Message[] KV = {new Message("001", "This is a good start, bingo!! bingo!!")};

    public static String getKV() {
      return KV[0].WORD;
    }
  }

  public static class Message {
    public final String ID;
    public final String WORD;

    public Message(String ID, String WORD) {
      this.ID = ID;
      this.WORD = WORD;
    }
  }

}
