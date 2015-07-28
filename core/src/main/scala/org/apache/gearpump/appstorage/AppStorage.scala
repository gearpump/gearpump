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
package org.apache.gearpump.appstorage

import java.io.File

import org.apache.gearpump._
import org.apache.gearpump.cluster.{ClusterConfig, ExecutorContext, UserConfig}
import org.apache.log4j.FileAppender

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

/**
 * The application storage interface. Provided by daemon and used by eah application,
 * This application storage needs to be thread-safe as multiple tasks can use it in parallel.
 */
trait AppStorage {
  /** initialize the app storage.
    * Shall only be called once for each application. */
  def initialize(appInfo: AppInfo, config: ClusterConfig) : Unit

  /** open the application storage for use */
  def open(appInfo: AppInfo) : Unit

  /** close this application storage */
  def close : Unit

  /** jar store */
  def getJarStore: JarStore

  /** log appender */
  def getLogAppender: FileAppender

  /** configuration */
  def getConfiguration(processId: Int): Option[UserConfig]
  def setConfiguration(processId: Int, conf: UserConfig): Unit

  /** create or get data store
    * @param name  the data store name, e.g. "checkpoint" or "offset"
    * @param subdir  the sub-directory this data store file will be in.
    *                E.g. user can set subdir as taskId so that different task has different data store.
    * */
  def getTimeSeriesDataStore(name: String, subdir: String): TimeSeriesDataStore
}

trait JarStore {
  def copyFromLocal(localFile: String): Unit
  def copyToLocal(remoteFile: String, localPath: String) : Unit
}

case class AppInfo(appName: String, appId: Int, user: String)
/**
 * This data store is used by application to persistently store time series data.
 * So, this data store can safely assume that the main trend of data writing is with monotonic timestamp.
 * It's possible that two data have the same timestamp, so this store needs
 * to handle this either during write or read.
 */
trait TimeSeriesDataStore {
  /**
   * try to look up the given time stamp,
   * return the corresponding Offset if the time is
   * in the range of stored TimeStamps or one of the
   * failure info (StorageEmpty, Overflow, Underflow)
   */
  def recover(time: TimeStamp): Try[Array[Byte]]

  /** append the data with the give time stamp*/
  def persist(time: TimeStamp, data: Array[Byte]): Unit

  def close(): Unit
}

object TimeSeriesDataStore {
  /**
   * StorageEmpty means no data has been stored
   */
  case object StorageEmpty extends Throwable

  /**
   * Overflow means the looked up time is
   * larger than the maximum stored TimeStamp
   * @param max Offset with the max TimeStamp
   */
  case class Overflow(max: Array[Byte]) extends Throwable

  /**
   * Underflow means the looked up time is
   * smaller than the minimum stored TimeStamp
   * @param min Offset with the min TimeStamp
   */
  case class Underflow(min: Array[Byte]) extends Throwable
}

case class AppStorageUndefinedException(message: String) extends Exception(message)

object AppStorage {
  /** called by master or worker to detect whether the configuration is correct */
  def getAppStorageObject(className: String): AppStorage = {
    if (className==null || className.isEmpty)
      throw AppStorageUndefinedException(s"Can't find application storage configuration at key $APPSTORAGE_CONFIG.")

    val clazz = getClass.getClassLoader.loadClass(className)
    clazz.asSubclass(AppStorage.getClass).newInstance().asInstanceOf[AppStorage]
  }

  /** called by master to create an application storage for an application when submitting application */
  def createAppStorage(appInfo: AppInfo, config: ClusterConfig): AppStorage = {
    val clazzName = config.worker.getString(APPSTORAGE_CONFIG)
    val appStorage: AppStorage = getAppStorageObject(clazzName)

    //initiate the class instance
    appStorage.initialize(appInfo, config)
    appStorageMap(appInfo.appId) = appStorage
    appStorage
  }

  /** called by each executor to get the application storage for this application */
  def getAppStorage(executorContext: ExecutorContext): Option[AppStorage] = {
    appStorageMap.get(executorContext.appId)
  }

  private val appStorageMap = new mutable.HashMap[Int, AppStorage]()

  private val APPSTORAGE_CONFIG = "gearpump.appstorage.class"
}
