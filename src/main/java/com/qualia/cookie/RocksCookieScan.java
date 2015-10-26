package com.qualia.cookie;


import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.common.base.Charsets;
import com.localresponse.util.CountMap;


public class RocksCookieScan {

    private static long dumpStats(long start, long num, String keyStr, String valStr) {
        long lastLog;
        lastLog = System.currentTimeMillis();

        double elap = (0.0 + System.currentTimeMillis() - start) / 1000.0;
        double putPerSec = (num + 0.0) / elap;
        double memUsed = (0.0 + Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
        System.out.println(String.format("Num %,12d   Elap %,8.1f   Num/Sec %,12.0f   MB %6.0f   Key %-40s   Value %s",
                num, elap, putPerSec, memUsed, keyStr, valStr));
        return lastLog;
    }


    public static void main(String[] args) throws RocksDBException, ParseException, IOException {
        Options options = new Options();
        options.addOption("d", "dbname", true, "RocksDb directory name (default test-db)");
        options.addOption("k", "dumpkeys", true, "Dump all keys to a file (default false)");
        options.addOption("f", "keyfile", true, "Filename for dumped keys (keys.txt)");
        options.addOption("h", "help", false, "Print usage help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("RocksCookieScan", options);
            return;
        }

        String dbName = cmd.getOptionValue("dbname", "test-db");
        boolean dumpKeys = Boolean.valueOf(cmd.getOptionValue("dumpkeys", "false"));
        String keyFile = cmd.getOptionValue("keyfile", "keys.txt");

        System.out.println("dbName = " + dbName);
        System.out.println("dumpKeys = " + dumpKeys);

        System.out.println("Starting");
        long start = System.currentTimeMillis();

        RocksDB.loadLibrary();
        RocksDB db = RocksDB.openReadOnly(dbName);

        RocksIterator iter = db.newIterator();
        iter.seekToFirst();

        long num = 0;
        long lastLog = System.currentTimeMillis();

        CountMap countMap = new CountMap();

        BufferedWriter bw = null;
        if (dumpKeys) {
            @SuppressWarnings("resource")
            FileOutputStream fs = new FileOutputStream(keyFile);
            OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
            bw = new BufferedWriter(osw);
        }

        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] val = iter.value();
            String keyStr = new String(key);
            String valStr = new String(val);
            if (keyStr.startsWith("adnxs")) {
                countMap.updateCount(valStr);
            }

            if (bw != null) {
                bw.write(keyStr);
                bw.newLine();
            }

            if ((num % 1000 == 0) && (System.currentTimeMillis() - lastLog >= 100)) {
                lastLog = dumpStats(start, num, keyStr, valStr);
            }

            iter.next();
            num++;
        }

        if (bw != null) {
            bw.close();
        }

        db.close();
        lastLog = dumpStats(start, num, null, null);

        System.out.println(countMap.report());

        System.out.println("Done");
    }

}
