package com.qualia.keystore_graph;


import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class CompactRocks {

    public static void main(String[] args) throws RocksDBException {
        String path;
        if (args.length > 0) {
            path = args[0];
        } else {
            path = "test-db";
        }

        boolean compress = false;
        if (args.length > 1) {
            compress = true;
        }

        System.out.println("Opening RocksDB = " + path);
        RocksDB db = RocksDB.open(Database.getDefaultOptions(compress), path);
        System.out.println("Compacting");
        db.compactRange();

        System.out.println("Closing");
        db.close();
        db.dispose();

        System.out.println("done");
    }

}
