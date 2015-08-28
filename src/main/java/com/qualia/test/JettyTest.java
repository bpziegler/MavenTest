package com.qualia.test;


import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;


public class JettyTest extends AbstractHandler {
    
    private final AtomicInteger counter = new AtomicInteger();

    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        int value = counter.incrementAndGet();
        response.getWriter().println("<h1>Hello World</h1> " + value);
        if (value % 100 == 0) {
            System.out.println("Received request:  " + value);
        }
    }


    public static void main(String[] args) throws Exception {
        Server server = new Server(8180);
        server.setHandler(new JettyTest());
        server.start();
        server.join();
    }

}
