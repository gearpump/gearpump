package org.apache.gearpump.util

import org.slf4j.{Logger, LoggerFactory}

object LogUtil {

  def getLogger[T](clazz : Class[T], context : String = null, master : Any = null, worker : Any = null, executor : Any = null, task : Any = null, app : Any = null) : Logger = {

    var env = ""

    if (null != context) {
      env += context
    }
    if (null != master) {
      env += "master" + master
    }
    if (null != worker) {
      env += "worker" + worker
    }

    if (null != app) {
      env += "app" + app
    }

    if (null != executor) {
      env += "exec" + executor
    }
    if (null != task) {
      env += task
    }

    if (!env.isEmpty) {
      LoggerFactory.getLogger(clazz.getSimpleName + "@" + env)
    } else {
      LoggerFactory.getLogger(clazz.getSimpleName)
    }
  }
}
