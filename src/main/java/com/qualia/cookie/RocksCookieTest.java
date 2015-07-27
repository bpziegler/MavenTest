package com.qualia.cookie;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
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
        Options options = new Options();
        options.addOption("d", "loaddir", true, "Directory with input cookie files");
        options.addOption("l", "loadthreads", true, "Number of load threads to parse the cookie files");
        options.addOption("c", "compactthreads", true, "Number of compaction threads");
        options.addOption("f", "flushthreads", true, "Number of flush threads");
        options.addOption("b", "bulkload", true, "Boolean to do bulk load (no compactions until after all loading)");
        options.addOption("h", "help", false, "Print usage help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        System.out.println("Options = " + Arrays.toString(cmd.getOptions()));
        String loadDir = cmd.getOptionValue("loaddir", "/Users/benziegler/test_data/cookies");
        System.out.println("loadDir = " + loadDir);
        int numLoadThread = Integer.valueOf(cmd.getOptionValue("loadthreads", "4"));
        int numCompactThreads = Integer.valueOf(cmd.getOptionValue("compactthreads", "4"));
        int numFlushThreads = Integer.valueOf(cmd.getOptionValue("flushthreads", "1"));
        int bulkLoad = Integer.valueOf(cmd.getOptionValue("bulkload", "0"));

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("RocksCookieTest", options);
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numLoadThread);
        final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000);
        final AtomicBoolean doneFlag = new AtomicBoolean();
        final AtomicInteger completedFiles = new AtomicInteger();

        List<File> list = new ArrayList<File>();
        FileRecur.getFilesInDirRecursively(loadDir, list);

        Thread thread = new Thread(new PutThread(queue, doneFlag, completedFiles, list.size(), numCompactThreads, numFlushThreads, bulkLoad));
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
