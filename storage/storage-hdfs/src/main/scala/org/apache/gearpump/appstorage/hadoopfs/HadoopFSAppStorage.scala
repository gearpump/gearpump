package org.apache.gearpump.appstorage.hadoopfs

import org.apache.gearpump.appstorage.hadoopfs.rotation.FileSizeRotation
import org.apache.gearpump.appstorage.{AppInfo, TimeSeriesDataStore, JarStore, AppStorage}
import org.apache.gearpump.cluster.{ClusterConfig, UserConfig}
import org.apache.log4j.FileAppender
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import HadoopFSAppStorage._

/**
 * Using Hadoop file system as Gearpump Application Storage
 */
class HadoopFSAppStorage extends AppStorage {
  var appInfoOption : Option[AppInfo] = None
  var clusterConfigOption: Option[ClusterConfig] = Option.empty
  var appPathOption: Option[String] = Option.empty
  var dataStoreMap = new scala.collection.mutable.HashMap[String, HadoopFSDataStore]
  val hadoopConf = getHadoopConfiguration
  var fileSystem : Option[FileSystem] = None
  /** initialize the app storage.
    * Shall only be called once for each application. */
  override def initialize(appInfo: AppInfo, config: ClusterConfig): Unit = {
    appInfoOption = Option(appInfo)
    clusterConfigOption = Option(config)

    //create directory on hadoop filesystem
    val fs = FileSystem.get(hadoopConf)
    val rootPathStr = clusterConfigOption.map(_.worker.getString(ROOT_PATH_KEY)).getOrElse(ROOT_PATH_DEFAULT_VAL)
    val appPathStr = s"$rootPathStr/${appInfo.appId}"
    appPathOption = Option(appPathStr)

    createDirectory(fs, appPathStr)
    createDirectory(fs, s"$appPathStr/$JARSTORE_DIR")
    createDirectory(fs, s"$appPathStr/$CONFIG_DIR")
    createDirectory(fs, s"$appPathStr/$LOG_DIR")
    createDirectory(fs, s"$appPathStr/$DATASTORE_DIR")

    fs.close()
  }

  private def createDirectory(fs: FileSystem, path: String): Boolean = {
    val p = new Path(path)

    //create root path if not exist
    if(!fs.exists(p))
      fs.mkdirs(p)

    true
  }

  /** data store */
  override def getTimeSeriesDataStore(name: String, subdir: String): TimeSeriesDataStore = {
    val key = s"$subdir/$name"
    val dataStoreRootDir = s"${appPathOption.get}/$DATASTORE_DIR"
    dataStoreMap.getOrElseUpdate(key, {
      val dataDir = s"$dataStoreRootDir/$subdir"
      createDirectory(fileSystem.get, dataDir)
      val rotation = new FileSizeRotation(10*1024*1024) //10MB
      val dataStore = new HadoopFSDataStore(appInfoOption.get,
          dataDir, name, hadoopConf, rotation, fileSystem.get)
      dataStore
    })
  }

  /** configuration */
  override def setConfiguration(processId: Int, conf: UserConfig): Unit = {
    //TODO
  }

  override def getConfiguration(processId: Int): Option[UserConfig] = {
    //TODO
    None
  }

  /** jar store */
  override def getJarStore: JarStore = {
    new HadoopFSJarStore(appInfoOption.get, s"${appPathOption.get}/$JARSTORE_DIR", hadoopConf)
  }

  /** open the application storage for use */
  override def open(appInfo: AppInfo): Unit = {
    val fs = FileSystem.get(hadoopConf)
    fileSystem = Option(fs)
  }

  /** close this application storage */
  override def close: Unit = {
    fileSystem.foreach(_.close())
  }

  /** log appender */
  override def getLogAppender: FileAppender = ???

  private def getHadoopConfiguration: Configuration = {
    new Configuration()
  }
}

object HadoopFSAppStorage {
  val ROOT_PATH_KEY = "gearpump.application.root.directory"
  val ROOT_PATH_DEFAULT_VAL = "/gearpump/application"

  val JARSTORE_DIR = "jarstore"
  val LOG_DIR = "log"
  val CONFIG_DIR = "config"
  val DATASTORE_DIR = "datastore"
}