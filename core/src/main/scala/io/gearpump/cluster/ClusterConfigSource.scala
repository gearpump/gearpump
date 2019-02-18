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

package io.gearpump.cluster

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import java.io.File
import scala.language.implicitConversions

/**
 * Data Source of ClusterConfig
 *
 * Please use ClusterConfigSource.apply(filePath) to construct this object
 */
sealed trait ClusterConfigSource extends Serializable {
  def getConfig: Config
}

object ClusterConfigSource {

  /**
   * Construct ClusterConfigSource from resource name or file path
   */
  def apply(filePath: String): ClusterConfigSource = {

    if (null == filePath) {
      new ClusterConfigSourceImpl(ConfigFactory.empty())
    } else {
      var config = ConfigFactory.parseFileAnySyntax(new File(filePath),
        ConfigParseOptions.defaults.setAllowMissing(true))

      if (null == config || config.isEmpty) {
        config = ConfigFactory.parseResourcesAnySyntax(filePath,
          ConfigParseOptions.defaults.setAllowMissing(true))
      }
      new ClusterConfigSourceImpl(config)
    }
  }

  implicit def FilePathToClusterConfigSource(filePath: String): ClusterConfigSource = {
    apply(filePath)
  }

  private class ClusterConfigSourceImpl(config: Config) extends ClusterConfigSource {
    override def getConfig: Config = config
  }
}