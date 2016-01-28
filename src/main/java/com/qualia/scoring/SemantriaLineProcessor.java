package com.qualia.scoring;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.jackson.map.ObjectMapper;

import com.qualia.keystore_graph.KeyStoreTable;


public class SemantriaLineProcessor implements Runnable {

    private final BlockingQueue<Object> queue;
    private final KeyStoreTable urlTopicsTable = new KeyStoreTable("url_topics", true, false);
    public final AtomicBoolean doneFlag = new AtomicBoolean();
    private final ObjectMapper mapper = new ObjectMapper();


    public SemantriaLineProcessor(BlockingQueue<Object> queue) {
        this.queue = queue;
    }


    @Override
    public void run() {
        try {
            while (true) {
                if (doneFlag.get() && queue.size() == 0) {
                    break;
                }
                Object obj = queue.poll(1, TimeUnit.MILLISECONDS);
                if (obj instanceof SemantriaLine) {
                    SemantriaLine line = (SemantriaLine) obj;
                    line.getLabelNames();
                    byte[] key = KeyStoreTable.getBytesForValue(line.getUrl());
                    String value = mapper.writeValueAsString(line.getLabelNames());
                    urlTopicsTable.put(key, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
