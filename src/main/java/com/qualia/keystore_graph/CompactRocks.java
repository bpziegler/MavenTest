package com.qualia.keystore_graph;


import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class CompactRocks {

    public static void main(String[] args) throws RocksDBException {
        String path;
        if (args.length > 0) {
            path = args[0];
        } else {
            path = "test-db/mapping";
        }

        boolean compress = false;
        if (args.length > 1) {
            compress = true;
        }

        System.out.println("Opening RocksDB = " + path);
        Options options = Database.getDefaultOptions(compress);
        options.setCreateIfMissing(false);
        RocksDB db = RocksDB.open(options, path);
        System.out.println("Compacting");
        db.compactRange();

        System.out.println("Closing");
        db.close();
        db.dispose();

        System.out.println("done");
    }

}
