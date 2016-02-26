package com.qualia.keystore_graph;


import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;


public class JsonSpeedTest {

    public static void main(String[] args) {
        final int NUM = 1000 * 1000;

        ObjectMapper mapper = new ObjectMapper();
        String s = "{\"a\":\"1234\", \"b\":\"hello!\"}";

        long startTime0 = System.nanoTime();

        for (int i = 0; i < 10*1000; i++) {
            try {
                JsonNode obj = mapper.readTree(s);
            } catch (JsonProcessingException e) {
            } catch (IOException e) {
            }
        }

        long elap0 = (System.nanoTime() - startTime0) / (1000 * 1000);
        System.out.println(String.format("Elap0 = %d", elap0));

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM; i++) {
            try {
                mapper.readTree(s);
                // s = "hello";
            } catch (Exception e) {
            }
        }

        long elap = (System.nanoTime() - startTime) / (1000 * 1000);
        System.out.println(String.format("Elap = %d", elap));
    }

}
