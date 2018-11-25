package io.gearpump.streaming

import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.Task
import io.gearpump.util.ReferenceEqual

import scala.reflect.ClassTag

/**
 * Processor is the blueprint for tasks.
 */
trait Processor[+T <: Task] extends ReferenceEqual {

  /**
   * How many tasks you want to use for this processor.
   */
  def parallelism: Int

  /**
   * The custom [[io.gearpump.cluster.UserConfig]], it is used to
   * initialize a task in runtime.
   */
  def taskConf: UserConfig

  /**
   * Some description text for this processor.
   */
  def description: String

  /**
   * The task class, should be a subtype of Task.
   *
   * Each runtime instance of this class is a task.
   */
  def taskClass: Class[_ <: Task]
}

object Processor {
  def ProcessorToProcessorDescription(id: ProcessorId, processor: Processor[_ <: Task])
    : ProcessorDescription = {
    import processor._
    ProcessorDescription(id, taskClass.getName, parallelism, description, taskConf)
  }

  def apply[T<: Task](
      parallelism : Int, description: String = "",
      taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T])
    : DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf,
      classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def apply[T<: Task](
      taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig)
    : DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  case class DefaultProcessor[T<: Task](
      parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T])
    extends Processor[T] {

    def withParallelism(parallel: Int): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallel, description, taskConf, taskClass)
    }

    def withDescription(desc: String): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallelism, desc, taskConf, taskClass)
    }

    def withConfig(conf: UserConfig): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallelism, description, conf, taskClass)
    }
  }
}