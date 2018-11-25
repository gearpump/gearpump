package io.gearpump.cluster

import akka.actor.ActorSystem

import scala.reflect.ClassTag

/**
 * Each job, streaming or not streaming, need to provide an Application class.
 * The master uses this class to start AppMaster.
 */
trait Application {

  /** Name of this application, must be unique in the system */
  def name: String

  /** Custom user configuration  */
  def userConfig(implicit system: ActorSystem): UserConfig

  /**
   * AppMaster class, must have a constructor like this:
   * this(appContext: AppMasterContext, app: AppDescription)
   */
  def appMaster: Class[_ <: ApplicationMaster]
}

object Application {
  def apply[T <: ApplicationMaster](
      name: String, userConfig: UserConfig)(implicit tag: ClassTag[T]): Application = {
    new DefaultApplication(name, userConfig,
      tag.runtimeClass.asInstanceOf[Class[_ <: ApplicationMaster]])
  }

  class DefaultApplication(
      override val name: String, inputUserConfig: UserConfig,
      val appMaster: Class[_ <: ApplicationMaster]) extends Application {
    override def userConfig(implicit system: ActorSystem): UserConfig = inputUserConfig
  }

  def ApplicationToAppDescription(app: Application)(implicit system: ActorSystem)
    : AppDescription = {
    val filterJvmReservedKeys = ClusterConfig.filterOutDefaultConfig(system.settings.config)
    AppDescription(app.name, app.appMaster.getName, app.userConfig, filterJvmReservedKeys)
  }
}