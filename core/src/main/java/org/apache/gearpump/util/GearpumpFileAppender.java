package org.apache.gearpump.util;

import org.apache.log4j.RollingFileAppender;
import java.net.InetAddress;

/**
 * A rolling appender with specific log file name (including hostname in filename)
 */
public class GearpumpFileAppender extends RollingFileAppender{
  private String hostname = null;
  private String logDir = null;

  ///set correct log file name and folder
  @Override
  public void activateOptions() {
    setDaemonLogFileName();
    super.activateOptions();
  }

  private void setDaemonLogFileName() {
    String prefix = fileName;
    String postfix = "";
    int dotPos = fileName.lastIndexOf('.');
    if(dotPos > 0) {
      prefix = fileName.substring(0,dotPos);
      postfix = fileName.substring(dotPos);
    }
    fileName = getLogDir() + "/"+ prefix + "-" + getHostname() + postfix;
  }

  ///return the hostname
  private String getHostname() {
    if(hostname==null) {
      try {
        hostname = InetAddress.getLocalHost().getHostName();
      }catch(Exception e) {
        hostname = "local";
      }
    }
    return hostname;
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
