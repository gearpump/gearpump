package org.apache.gearpump.service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xzhong10 on 2014/7/21.
 */

public class SimpleKVHandler extends HttpServlet {
    private ConcurrentHashMap<String, String> kvs = new ConcurrentHashMap<String, String>();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        String key = request.getParameter("key");


        if (key != null) {
            String value = request.getParameter("value");
            if (null != value) {
                kvs.put(key, value);
            } else {
                String returnValue = kvs.get(key);
                if (null != returnValue) {
                    response.getWriter().write(returnValue);
                }
            }
        }
    }
}