package com.qualia.keystore_graph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public class KeyStoreTable {
	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final boolean DEFAULT_WRITE_TO_WAL = false;

	private final RocksDB db;
	private WriteBatch writeBatch;
	private int numBatch = 0;
	private boolean writeToWAL = DEFAULT_WRITE_TO_WAL;

	public KeyStoreTable(String tableName, boolean compress, boolean readOnly) {
		File dir = new File("test-db", tableName);
		try {
			this.db = Database.getDb(dir.getAbsolutePath(), compress, readOnly);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}

		writeBatch = new WriteBatch();
	}

	public void put(byte[] key, Object value) throws RocksDBException {
		writeBatch.put(key, getBytesForValue(value));
		numBatch++;

		checkFlush(DEFAULT_BATCH_SIZE);
	}

	public void putNoBatch(byte[] key, Object value) throws RocksDBException {
		db.put(key, getBytesForValue(value));
	}

	public void scan(byte[] startKey, IScanCallback scanCallback) {
		RocksIterator iter = db.newIterator();
		
		iter.seek(startKey);

		while (iter.isValid()) {
			byte[] key = iter.key();
			byte[] val = iter.value();

			boolean keepGoing = scanCallback.onRow(key, val);
			if (!keepGoing) {
				break;
			}

			iter.next();
		}
		
		iter.dispose();
	}

	private void checkFlush(int checkSize) throws RocksDBException {
		if (numBatch >= checkSize) {
			WriteOptions writeOptions = new WriteOptions();
			writeOptions.setSync(false);
			if (isWriteToWAL() == false) {
				writeOptions.setDisableWAL(true);
			}

			db.write(writeOptions, writeBatch);
			writeBatch.dispose();
			writeOptions.dispose();
			numBatch = 0;
			writeBatch = new WriteBatch();
		}
	}

	public String getString(byte[] key) {
		return null;
	}

	public int getInt(byte[] key) {
		return 0;
	}

	public void flush() {
		try {
			checkFlush(1);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		flush();
		writeBatch.dispose();
	}

	public static byte[] getBytesForValue(Object value) {
		try {
			byte[] valueBytes = null;
			if (value instanceof String) {
				valueBytes = ((String) value).getBytes(Charsets.UTF_8);
			} else if (value instanceof Integer) {
				ByteArrayOutputStream valStream = new ByteArrayOutputStream();
				DataOutputStream ds = new DataOutputStream(valStream);
				ds.writeInt((Integer) value);
				valueBytes = valStream.toByteArray();
            } else if (value instanceof Long) {
                ByteArrayOutputStream valStream = new ByteArrayOutputStream();
                DataOutputStream ds = new DataOutputStream(valStream);
                ds.writeLong((Long) value);
                valueBytes = valStream.toByteArray();
			} else if (value instanceof byte[]) {
				valueBytes = (byte[]) value;
			}
			Preconditions.checkState(valueBytes != null, "Invalid value " + value);
			return valueBytes;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isWriteToWAL() {
		return writeToWAL;
	}

	public void setWriteToWAL(boolean writeToWAL) {
		this.writeToWAL = writeToWAL;
	}

}
