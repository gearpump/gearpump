package org.apache.gearpump.cluster

import scala.reflect.ClassTag

// These types must all be easily serde by upickle
object Messages {
  type TimeStamp = Long
  type ProcessorId = Int
  type TaskIndex = Int
  type ExecutorId = Int

  trait Graph[N,E]

  trait UserConfig

  case class Message(msg: AnyRef, timestamp: TimeStamp = Message.noTimeStamp)

  object Message {
    val noTimeStamp : TimeStamp = 0L
  }

  case class TaskId(processorId : ProcessorId, index : TaskIndex)

  object TaskId {
    def toLong(id : TaskId) = (id.processorId.toLong << 32) + id.index
    def fromLong(id : Long) = TaskId(((id >> 32) & 0xFFFFFFFF).toInt, (id & 0xFFFFFFFF).toInt)
  }
  trait Partitioner extends Serializable {
    def getPartition(msg : Message, partitionNum : Int, currentPartitionId: Int) : Int

    def getPartition(msg : Message, partitionNum : Int) : Int = {
      getPartition(msg, partitionNum, Partitioner.UNKNOWN_PARTITION_ID)
    }
  }

  sealed trait PartitionerFactory {
    def partitioner: Partitioner
  }

  trait ReferenceEqual extends AnyRef {

    /**
     * Equal based on reference Id
     */
    override def equals(other : Any) : Boolean = {
      this.eq(other.asInstanceOf[AnyRef])
    }
  }

  case class PartitionerObject(partitioner: Partitioner) extends PartitionerFactory

  case class PartitionerByClassName(partitionerClass: String) extends PartitionerFactory {
    override def partitioner: Partitioner = Class.forName(partitionerClass).newInstance().asInstanceOf[Partitioner]
  }

  case class LifeTime(birth: Long, die: Long)

  object LifeTime {
    val Immortal = LifeTime(0L, Long.MaxValue)
  }

  case class PartitionerDescription(partitionerFactory: PartitionerFactory, life: LifeTime = LifeTime.Immortal)

  object Partitioner {
    val UNKNOWN_PARTITION_ID = -1

    def apply[T <: Partitioner](implicit clazz: ClassTag[T]): PartitionerDescription = {
      PartitionerDescription(PartitionerByClassName(clazz.runtimeClass.getName))
    }
  }

  case class ProcessorDescription(id: ProcessorId, taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null, life: LifeTime = LifeTime.Immortal) extends ReferenceEqual


  trait AppMasterDataDetail {
    def appId: Int
    def appName: String
    def actorPath: String
    def executors: Map[Int, String]
  }

  case class GeneralAppMasterDataDetail(appId: Int, appName: String = null, actorPath: String = null,
                                        executors: Map[Int, String] = Map.empty[Int, String]) extends AppMasterDataDetail

  case class StreamingAppMasterDataDetail(appId: Int, appName: String = null, processors: Map[ProcessorId, ProcessorDescription],
                                           // hierarchy level for each processor
                                           processorLevels: Map[ProcessorId, Int],
                                           dag: Graph[ProcessorId, PartitionerDescription] = null,
                                           actorPath: String = null,
                                           clock: TimeStamp = 0,
                                           executors: Map[ExecutorId, String] = null,
                                           tasks: Map[TaskId, ExecutorId] = null) extends AppMasterDataDetail
}
