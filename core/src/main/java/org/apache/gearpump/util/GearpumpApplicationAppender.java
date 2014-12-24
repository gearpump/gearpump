package org.apache.gearpump.util;

import org.apache.log4j.RollingFileAppender;

import java.net.InetAddress;

/**
 * Log4j appender for application (executor).
 */
public class GearpumpApplicationAppender extends RollingFileAppender {
  private String hostname = null;
  private String logDir = null;

  ///set correct log file name and folder
  @Override
  public void activateOptions() {
    setExecutorLogFileName();

    super.activateOptions();
  }

  private void setExecutorLogFileName() {
    String appId = getSystemEnv("application");
    String executorId = getSystemEnv("executor");
    fileName = getLogDir() + "/applicationData/app!" + appId + "/executor!" + executorId + ".log";
  }

  private String getSystemEnv(String name) {
    return System.getProperty(name);
  }

  ///return log folder
  private String getLogDir() {
    if(logDir==null) {
      logDir = System.getenv("GEARPUMP_LOG_DIR");
      if(logDir==null)
        logDir = "logs";
    }
    return logDir;
  }
}
