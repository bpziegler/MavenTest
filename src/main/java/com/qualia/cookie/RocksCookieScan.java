package com.qualia.cookie;


import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.localresponse.util.CountMap;


public class RocksCookieScan {

    public static void main(String[] args) throws RocksDBException {
        System.out.println("Starting");
        long start = System.currentTimeMillis();

        RocksDB.loadLibrary();
        RocksDB db = RocksDB.open("test-db");

        RocksIterator iter = db.newIterator();
        iter.seekToFirst();

        long num = 0;
        long lastLog = System.currentTimeMillis();

        CountMap countMap = new CountMap();

        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] val = iter.value();
            String valStr = new String(val);
            countMap.updateCount(valStr);

            if ((num % 1000 == 0) && (System.currentTimeMillis() - lastLog >= 100)) {
                lastLog = System.currentTimeMillis();

                double elap = (0.0 + System.currentTimeMillis() - start) / 1000.0;
                double putPerSec = (num + 0.0) / elap;
                double memUsed = (0.0 + Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
                        / (1024 * 1024);
                String keyStr = new String(key);
                System.out.println(String.format(
                        "Num %,12d   Elap %,8.1f   Num/Sec %,12.0f   MB %6.0f   Key %-40s   Value %s", num, elap,
                        putPerSec, memUsed, keyStr, valStr));
            }

            iter.next();
            num++;
        }

        db.close();

        System.out.println(countMap.report());

        System.out.println("Done");
    }

}
