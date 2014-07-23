package org.apache.gearpump.service

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
