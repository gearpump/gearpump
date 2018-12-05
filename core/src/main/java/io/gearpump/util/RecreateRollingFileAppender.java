/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.util;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.io.File;

/** When log file is deleted, tried to recreate it in file system  */
public class RecreateRollingFileAppender extends RollingFileAppender {

  protected long checkFileInterval = 60L;
  private long lastCheckTime = 0L;

  @Override
  public void append(LoggingEvent event) {
    checkInterval();
    super.append(event);
  }

  private void checkInterval() {
    long currentTime = System.currentTimeMillis();
    if ((currentTime - lastCheckTime) > (checkFileInterval * 1000)) {
      checkLogFileExist();
      lastCheckTime = currentTime;
    }
  }

  private void checkLogFileExist() {
    String fileName = super.fileName;
    if (fileName != null) {
      File logFile = new File(fileName);
      if (!logFile.exists()) {
        this.setFile(fileName);
        this.activateOptions();
      }
    }
  }

  public long getCheckFileInterval() {
    return this.checkFileInterval;
  }

  public void setCheckFileInterval(long checkFileInterval) {
    this.checkFileInterval = checkFileInterval;
  }
}
