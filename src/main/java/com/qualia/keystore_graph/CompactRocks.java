package com.qualia.keystore_graph;


import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
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

        Options options = new Options();
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setIncreaseParallelism(4);
        options.setMaxBackgroundCompactions(2);
        options.setMaxBackgroundFlushes(2);
        options.setWriteBufferSize(64 * 1024 * 1024);
        System.out.println("Opening RocksDB = " + path);
        RocksDB db = RocksDB.open(options, path);
        System.out.println("Compacting");
        db.compactRange();
        System.out.println("done");
    }

}
