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

package org.apache.gearpump.cluster.master

import org.apache.gearpump.cluster.{AppJar, AppDescription}

/**
  * This state will be persisted across the masters.
  */
case class ApplicationState(appId : Int, appName: String, attemptId : Int, app : AppDescription, jar: Option[AppJar], username : String, state : Any) extends Serializable {

   override def equals(other: Any): Boolean = {
     other match {
       case that: ApplicationState =>
         if (appId == that.appId && attemptId == that.attemptId) {
           true
         } else {
           false
         }
       case _ =>
         false
     }
   }

   override def hashCode: Int = {
     import akka.routing.MurmurHash._
     extendHash(appId, attemptId, startMagicA, startMagicB)
   }
 }
