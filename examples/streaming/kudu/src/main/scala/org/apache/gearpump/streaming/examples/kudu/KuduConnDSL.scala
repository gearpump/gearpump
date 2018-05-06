/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.examples.kudu

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.external.kudu.KuduSink
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.util.AkkaApp

object KuduConnDSL extends AkkaApp with ArgumentsParser {
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val map = Map[String, String]("KUDUSINK" -> "kudusink", "kudu.masters" -> "localhost",
      "KUDU_USER" -> "kudu.user", "GEARPUMP_KERBEROS_PRINCIPAL" -> "gearpump.kerberos.principal",
      "GEARPUMP_KEYTAB_FILE" -> "gearpump.keytab.file", "TABLE_NAME" -> "kudu.table.name"
    )

    val userConfig = new UserConfig(map)
    val appName = "KuduDSL"
    val context = ClientContext(akkaConf)
    val app = StreamApp(appName, context)

    app.source(new Split).sink(new KuduSink(userConfig, "impala::default.kudu_1"), 1,
      userConfig, "KuduSink" )

    context.submit(app)
    context.close()
  }
}
