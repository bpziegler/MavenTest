package com.qualia.scoring;


import java.util.concurrent.BlockingQueue;


public class SemantriaLineProcessor implements Runnable {

    private final BlockingQueue<Object> queue;


    public SemantriaLineProcessor(BlockingQueue<Object> queue) {
        this.queue = queue;
    }


    @Override
    public void run() {
    }

}
