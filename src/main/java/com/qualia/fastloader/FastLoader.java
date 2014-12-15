package com.qualia.fastloader;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.iq80.leveldb.*;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;


public class FastLoader {

    private static final int BATCH_SIZE = 1000;

    private final Splitter tabSplitter = Splitter.on("\t");
    private final Splitter commaSplitter = Splitter.on(",");
    private final Joiner tabJoiner = Joiner.on("\t");
    private DB db;
    private final byte[] ONE = "1".getBytes();
    private WriteBatch batch;
    private int numBatch;


    private void run(String srcFile) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(new File("fastload-db"), options);
        batch = db.createWriteBatch();

        FileInputStream fs = new FileInputStream(srcFile);
        GZIPInputStream gzip = new GZIPInputStream(fs);
        InputStreamReader reader = new InputStreamReader(gzip);
        BufferedReader br = new BufferedReader(reader);

        long lineNum = 0;
        numBatch = 0;
        String line = null;
        long startTime = System.currentTimeMillis();

        while ((line = br.readLine()) != null) {
            parseLine(line, lineNum);
            lineNum++;

            numBatch++;
            if (numBatch >= BATCH_SIZE) {
                db.write(batch);
                batch = db.createWriteBatch();
                numBatch = 0;
            }

            if (lineNum % 50000 == 0) {
                double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000;
                double linePerSec = lineNum / elap;
                System.out.println(String.format("Line %,12d   Elap %5.1f   Line/Sec %,10.0f", lineNum, elap,
                        linePerSec));
            }
        }

        br.close();

        System.out.println("Closing DB");
        db.close();
        System.out.println("Done");
    }


    private void parseLine(String line, long lineNum) {
        if (lineNum < 100) {
            System.out.println(line);
        }

        List<String> columns = tabSplitter.splitToList(line);
        if (columns.size() >= 3) {
            String cookies = columns.get(2);
            List<String> cookieParts = commaSplitter.splitToList(cookies);

            String normalOrder = tabJoiner.join(cookieParts);
            batch.put(normalOrder.getBytes(), ONE);

            String reverseOrder = tabJoiner.join(Lists.reverse(cookieParts));
            batch.put(reverseOrder.getBytes(), ONE);
        }
    }


    public static void main(String[] args) throws IOException {
        final String srcFile = "/Users/benziegler/test_data/addthis_map_test.txt.gz";
        FastLoader fastLoader = new FastLoader();
        fastLoader.run(srcFile);
    }

}
