package com.qualia.cookie;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.rocksdb.RocksDBException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.io.CountingInputStream;
import com.qualia.util.FileRecur;


public class RocksCookieTest implements Runnable {

    private final ObjectMapper mapper = new ObjectMapper();

    private final BlockingQueue<Object> queue;
    private AtomicInteger completedFiles;
    private final String path;

    private long numCookie;


    public RocksCookieTest(BlockingQueue<Object> queue, AtomicInteger completedFiles, String path) {
        this.queue = queue;
        this.path = path;
        this.completedFiles = completedFiles;
    }


    @Override
    public void run() {
        try {
            runWorker();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void runWorker() throws IOException, RocksDBException {
        FileInputStream fs = new FileInputStream(path);
        CountingInputStream cs = new CountingInputStream(fs);
        InputStream nextStream = cs;
        nextStream = new GZIPInputStream(nextStream);
        InputStreamReader isr = new InputStreamReader(nextStream);
        BufferedReader br = new BufferedReader(isr, 256 * 1024);

        String line;
        long curLine = 0;

        while ((line = br.readLine()) != null) {
            processLine(line, curLine);
            curLine++;

            if (curLine % 10000 == 0) {
                // System.out.println(String.format("%30s  %,10d", path, curLine));
            }
        }

        br.close();
        completedFiles.getAndIncrement();
    }


    protected void processLine(String line, long curLine) {
        try {
            JsonNode tree = mapper.readTree(line);

            ArrayNode mapping = (ArrayNode) tree.get("mapping");
            if (mapping == null)
                return;
            // time = "2015-03-24T00:09:44.935Z"
            String time = tree.get("time").asText();
            String dateOnly = time.substring(0, 10).replace("-", "");
            // System.out.println(time + "   " + dateOnly);
            for (JsonNode oneMapping : mapping) {
                numCookie++;
                String pid = oneMapping.get("pid").asText();
                String uid = oneMapping.get("uid").asText();
                String pid_uid = pid + "_" + uid;
                String val = dateOnly;

                PutKeyValue putKeyValue = new PutKeyValue(pid_uid.getBytes(), val.getBytes());
                queue.put(putKeyValue);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000);
        final AtomicBoolean doneFlag = new AtomicBoolean();
        final AtomicInteger completedFiles = new AtomicInteger();

        String dir = "/Users/benziegler/test_data/cookies";
        if (args.length > 0) {
            dir = args[0];
            System.out.println("dir = " + dir);
        }

        List<File> list = new ArrayList<File>();
        FileRecur.getFilesInDirRecursively(dir, list);

        Thread thread = new Thread(new PutThread(queue, doneFlag, completedFiles, list.size()));
        thread.start();

        for (File oneFile : list) {
            RocksCookieTest program = new RocksCookieTest(queue, completedFiles, oneFile.getAbsolutePath());
            executor.submit(program);
        }

        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.DAYS);

        doneFlag.set(true);
        thread.join();

        System.out.println("Done");
    }

}
