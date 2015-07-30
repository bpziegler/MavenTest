package com.qualia.cookie;


import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class RocksCompact {

    public static void main(String[] args) throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setIncreaseParallelism(4);
        options.setMaxBackgroundCompactions(2);
        options.setMaxBackgroundFlushes(2);

        String dbDir = args[0];
        if (dbDir == null) {
            dbDir = "test-db";
        }

        System.out.println("Opening DB:  " + dbDir);
        RocksDB db = RocksDB.open(options, dbDir);

        System.out.println("Compacting DB");
        db.compactRange();

        System.out.println("Closing");
        db.close();
        db.dispose();

        System.out.println("Done");
    }

}
