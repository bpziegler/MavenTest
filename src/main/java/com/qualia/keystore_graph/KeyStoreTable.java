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
	private final RocksDB db;
	private WriteBatch writeBatch;
	private WriteOptions writeOptions;
	private int numBatch = 0;

	public KeyStoreTable(String tableName, boolean compress, boolean readOnly) {
		File dir = new File("test-db", tableName);
		try {
			this.db = Database.getDb(dir.getAbsolutePath(), compress, readOnly);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}

		writeOptions = new WriteOptions();
		writeOptions.setSync(false);
		writeOptions.setDisableWAL(true);

		writeBatch = new WriteBatch();
	}

	public void put(byte[] key, Object value) throws RocksDBException {
		writeBatch.put(key, getBytesForValue(value));
		numBatch++;

		checkFlush(1000);
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
			db.write(writeOptions, writeBatch);
			writeBatch.dispose();
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
			} else if (value instanceof byte[]) {
				valueBytes = (byte[]) value;
			}
			Preconditions.checkState(valueBytes != null, "Invalid value " + value);
			return valueBytes;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
