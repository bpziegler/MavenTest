package com.qualia.dedup_addthis;


import java.io.IOException;
import java.util.List;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qualia.keystore_graph.KeyStoreTable;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class DedupAddThis {

    private RocksDB db;
    private static final Splitter tabSplitter = Splitter.on("\t");
    private final HashFunction hashFunction = Hashing.murmur3_128();
    private long numNew;
    private long numDup;


    public DedupAddThis(String dbPath) throws RocksDBException {
        db = initDb(dbPath);
    }


    private RocksDB initDb(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        // options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setCreateIfMissing(true);
        options.setIncreaseParallelism(4);
        options.setMaxBackgroundCompactions(2);
        options.setMaxBackgroundFlushes(2);
        RocksDB db = RocksDB.open(options, dbPath);
        return db;
    }


    private void run(String loadDir) throws IOException {
        MultiFileLineProcessor processor = new MultiFileLineProcessor();
        processor.setUseGzip(true);

        ILineProcessor lineProcessor = new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    DedupAddThis.this.processLine(line, curLine);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }


            @Override
            public String getStatus() {
                return String.format("New %,10d   Dups %,10d", numNew, numDup);
            }
        };

        processor.processDir(loadDir, lineProcessor);
    }


    protected void processLine(String line, long curLine) throws RocksDBException {
        List<String> parts = tabSplitter.splitToList(line);
        // 1442115450535
        String timestampStr = parts.get(0);
        long timestamp = Long.valueOf(timestampStr);
        // 6=8828144767917942418,9=5467eeed74ec7d39,11127=d4895758814e4b3033c0952a39628669
        String cookieParts = parts.get(2);
        byte[] hashOfAddThisMapping = hashFunction.hashString(cookieParts, Charsets.UTF_8).asBytes();
        byte[] value = db.get(hashOfAddThisMapping);
        if (value == null) {
            numNew++;
            db.put(hashOfAddThisMapping, KeyStoreTable.getBytesForValue(timestamp));
            // TODO:  Output a mapping line to be processed by Cookie worker
        } else {
            numDup++;
            // TODO:  Output a last_seen line to be processed by LastSeen worker
        }
    }


    public static void main(String[] args) throws RocksDBException, IOException {
        DedupAddThis program = new DedupAddThis("dedup-rocks-db");
        String loadDir = args[0];
        program.run(loadDir);
    }
}
