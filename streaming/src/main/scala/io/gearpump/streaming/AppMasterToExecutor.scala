package io.gearpump.streaming

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.appmaster.TaskRegistry.TaskLocations
import io.gearpump.streaming.task.{Subscriber, TaskId}

object AppMasterToExecutor {
  case class LaunchTasks(
      taskId: List[TaskId], dagVersion: Int, processorDescription: ProcessorDescription,
      subscribers: List[Subscriber])

  case object TasksLaunched

  /**
   * dagVersion, life, and subscribers will be changed on target task list.
   */
  case class ChangeTasks(
      taskId: List[TaskId], dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case class TasksChanged(taskIds: List[TaskId])

  case class ChangeTask(
      taskId: TaskId, dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case class TaskChanged(taskId: TaskId, dagVersion: Int)

  case class StartTask(taskId: TaskId)

  case class StopTask(taskId: TaskId)

  case class TaskLocationsReady(taskLocations: TaskLocations, dagVersion: Int)

  case class TaskLocationsReceived(dagVersion: Int, executorId: ExecutorId)

  case class TaskLocationsRejected(
      dagVersion: Int, executorId: ExecutorId, reason: String, ex: Throwable)

  case class StartAllTasks(dagVersion: Int)

  case class StartDynamicDag(dagVersion: Int)
  case class TaskRegistered(taskId: TaskId, sessionId: Int, startClock: MilliSeconds)
  case class TaskRejected(taskId: TaskId)

  case object RestartClockService
  class MsgLostException extends Exception
}
