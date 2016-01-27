package com.qualia.scoring;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.qualia.keystore_graph.Status;
import com.qualia.keystore_graph.StatusThread;
import com.qualia.util.FileRecur;


public class SemantriaMT {

    private void run(String dirPath) throws InterruptedException {
        System.out.println("Started at " + DateTime.now());
        System.out.println("Scanning Files Dir = " + dirPath);

        int cores = Runtime.getRuntime().availableProcessors();
        final ExecutorService executor = Executors.newFixedThreadPool(cores);
        final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000);

        List<File> files = new ArrayList<File>();
        FileRecur.getFilesInDirRecursively(dirPath, files);

        long totBytes = 0;
        for (File oneFile : files) {
            totBytes += oneFile.length();
        }

        System.out.println(String.format("Files %,d   Bytes %,d", files.size(), totBytes));

        Status status = new Status();
        status.totBytes.set(totBytes);
        status.totFiles.set(files.size());
        StatusThread statusThread = new StatusThread(status);
        statusThread.start();

        for (File oneFile : files) {
            ParseSemantriaThread thread = new ParseSemantriaThread(oneFile.getAbsolutePath(), queue, status);
            executor.submit(thread);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        statusThread.done.set(true);
        statusThread.join();

        System.out.println("Finished at " + DateTime.now());
    }


    public static void main(String[] args) throws Exception {
        SemantriaMT program = new SemantriaMT();
        program.run(args[0]);
    }

}
