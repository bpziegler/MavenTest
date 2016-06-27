package com.qualia.keystore_graph;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.qualia.scoring.MD5Helper;

public class JsonParseTest {
    
    public static class Test {
        public int a = 1;
        public int b = 2;
        public String name = "ben";
        public List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
    }

    public static void main(String[] args) throws Exception {
        final int NUM = 1000 * 1000;

        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(new Test());
        System.out.println(s);

        long startTime0 = System.nanoTime();

        for (int i = 0; i < 10 * 1000; i++) {
            try {
                mapper.readValue(s, Test.class);
            } catch (JsonProcessingException e) {
            } catch (IOException e) {
            }
        }

        long elap0 = (System.nanoTime() - startTime0) / (1000 * 1000);
        System.out.println(String.format("Elap0 = %d", elap0));

        long startTime = System.nanoTime();

        MD5Helper helper = new MD5Helper();

        for (int i = 0; i < NUM; i++) {
            try {
                mapper.readValue(s, Test.class);
                // s = helper.stringToMD5Hex(String.valueOf(i));
            } catch (Exception e) {
            }
        }

        long elap = (System.nanoTime() - startTime) / (1000 * 1000);
        System.out.println(String.format("Elap = %d", elap));
    }

}
