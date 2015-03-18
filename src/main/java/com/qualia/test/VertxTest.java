package com.qualia.test;


import org.vertx.java.core.Handler;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;


public class VertxTest extends Verticle {

    private long counter;
    private long firstHit = 0;


    public static void main(String[] args) {
    }


    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();

        server.requestHandler(new Handler<HttpServerRequest>() {

            public void handle(HttpServerRequest request) {
                counter++;
                long time = System.currentTimeMillis();
                if (firstHit == 0) {
                    firstHit = time;
                }
                if (counter % 2000 == 0) {
                    double perSec = (counter + 0d) / (time - firstHit) * 1000;
                    System.out.println(counter + "   time = " + time + "   perSec = " + perSec);
                }
                request.response().end("Hello World from vertx");
            }
        });

        server.listen(8080);
    }

}
