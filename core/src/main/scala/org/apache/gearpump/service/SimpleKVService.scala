package org.apache.gearpump.service
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, NameValuePair}
import org.eclipse.jetty.server.handler.{HandlerCollection, ResourceHandler}
import org.eclipse.jetty.server.{Handler, Server}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}


object SimpleKVService {
  private var url : String = null

  def main (args: Array[String]) {
    val port = args(0).toInt
    val server = new Server(port)

    val resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    resourceHandler.setResourceBase(".");

    val servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    servletContextHandler.setContextPath("/");
    servletContextHandler.addServlet(new ServletHolder(new SimpleKVHandler()), "/kv");

    val handlerList = new HandlerCollection();
    handlerList.setHandlers(Array[Handler](servletContextHandler, resourceHandler));
    server.setHandler(handlerList);

    server.start();
    server.join()
  }

  /**
   * kvServiceURL: key value service URL
   * To set a value, send a GET request to http://{kvServiceURL}?key={key}&value={value}
   * to get a value, send a GET request to http://{kvServiceURL}?key={key}
   */
  def init(url : String) {
    SimpleKVService.url = url
  }

  def get(key : String) : String = {
    val httpClient : HttpClient = new HttpClient();
    val get : GetMethod = new GetMethod(url + "?" + "key=" + key)
    httpClient.executeMethod(get)
    get.getResponseBodyAsString()
  }

  def set(key : String, value : String) : Unit = {
    val httpClient : HttpClient = new HttpClient();
    val get : GetMethod = new GetMethod(url)
    get.setQueryString(Array[NameValuePair](
      new NameValuePair("key", key),new NameValuePair("value", value)
    ))
    httpClient.executeMethod(get)
    get.getResponseBodyAsString()
  }

}
