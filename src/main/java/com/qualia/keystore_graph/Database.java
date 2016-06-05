package com.qualia.keystore_graph;

import java.util.HashMap;
import java.util.Map;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Database {

    private static final Object LOCK = new Object();
    private static final Map<String, RocksDB> dbMap = new HashMap<String, RocksDB>();

    public static RocksDB getDb(String path, boolean compress, boolean readOnly) throws RocksDBException {
        synchronized (LOCK) {
            RocksDB result = dbMap.get(path);
            if (result == null) {
                result = initDb(path, compress, readOnly);
                dbMap.put(path, result);
            }
            return result;
        }
    }

    public static void close() {
        synchronized (LOCK) {
            for (RocksDB db : dbMap.values()) {
                db.close();
                db.dispose();
            }
        }
    }

    private static RocksDB initDb(String path, boolean compress, boolean readOnly) throws RocksDBException {
        synchronized (LOCK) {
            RocksDB.loadLibrary();
            RocksDB db;

            if (readOnly) {
                db = RocksDB.openReadOnly(path);
            } else {
                Options options = getDefaultOptions(compress);

                db = RocksDB.open(options, path);
            }

            return db;
        }
    }

    public static Options getDefaultOptions(boolean compress) {
        Options options = new Options();
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        if (compress) {
            options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        }
        options.setCreateIfMissing(true);
        options.setIncreaseParallelism(4);
        options.setMaxBackgroundCompactions(2);
        options.setMaxBackgroundFlushes(2);
        options.setWriteBufferSize(8 * 1024 * 1024);
        return options;
    }

}
