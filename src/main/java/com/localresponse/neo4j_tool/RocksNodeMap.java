package com.localresponse.neo4j_tool;


import java.io.File;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.qualia.keystore_graph.Database;
import com.qualia.keystore_graph.KeyStoreTable;


public class RocksNodeMap implements INodeIdMap {

    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final RocksDB db;
    private WriteBatch writeBatch;
    private int numBatch;


    public RocksNodeMap() {
        File dir = new File("node-id-map");
        try {
            this.db = Database.getDb(dir.getAbsolutePath(), true, false);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        writeBatch = new WriteBatch();
    }


    @Override
    public void put(long oldId, long newId) {
        byte[] oldIdBytes = KeyStoreTable.getBytesForValue(oldId);
        byte[] newIdBytes = KeyStoreTable.getBytesForValue(newId);

        writeBatch.put(oldIdBytes, newIdBytes);
        numBatch++;

        try {
            checkFlush(DEFAULT_BATCH_SIZE);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public long get(long oldId) {
        try {
            checkFlush(1);
            byte[] oldIdBytes = KeyStoreTable.getBytesForValue(oldId);
            byte[] valBytes = db.get(oldIdBytes);
            return KeyStoreTable.getLong(valBytes);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


    private void checkFlush(int checkSize) throws RocksDBException {
        if (numBatch >= checkSize) {
            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);

            db.write(writeOptions, writeBatch);
            writeBatch.dispose();
            writeOptions.dispose();
            numBatch = 0;
            writeBatch = new WriteBatch();
        }
    }

}
