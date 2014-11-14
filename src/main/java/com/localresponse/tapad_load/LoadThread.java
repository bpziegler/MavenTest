package com.localresponse.tapad_load;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class LoadThread extends Thread {

    private final BlockingQueue<LoadTask> tasks;
    private final AtomicBoolean done;


    public LoadThread(BlockingQueue<LoadTask> tasks, AtomicBoolean done) {
        this.tasks = tasks;
        this.done = done;
    }


    public void run() {
        while (!done.get()) {
            try {
                LoadTask value = tasks.poll(100, TimeUnit.MILLISECONDS);

                if (value != null) {
                    value.run();
                }
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

}
