/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.hadoop.lib

package object rotation {




  implicit final class FileSizeConversions(size: Int) {
    def B = new BSize(size)

    def KB = new KBSize(size)

    def MB = new MBSize(size)

    def GB = new GBSize(size)

    def TB = new TBSize(size)
  }

  sealed trait FileSize {
    def toBytes: Long
  }

  class BSize(size: Int) extends FileSize {
    override def toBytes: Long = size
  }

  class KBSize(size: Int) extends FileSize {
    override def toBytes: Long = size * Math.pow(2, 10).toLong
  }

  class MBSize(size: Int) extends FileSize {
    override def toBytes: Long = size * Math.pow(2, 20).toLong
  }

  class GBSize(size: Int) extends FileSize {
    override def toBytes: Long = size * Math.pow(2, 30).toLong
  }

  class TBSize(size: Int) extends FileSize {
    override def toBytes: Long = size * Math.pow(2, 40).toLong
  }

}
