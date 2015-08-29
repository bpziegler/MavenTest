package com.qualia.test;

import java.util.Random;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksTest {

	private static final int NUM_TEST = 800 * 1000 * 1000;

	private static void dumpStats(long start, long i, String desc) {
		long elap = System.currentTimeMillis() - start;
		double put_per_sec = (i + 0.0) / (elap / 1000.0);
		System.out.println(String.format("Num = %,12d   Elap = %8.3f   %8s = %,12.0f", i, (elap + 0.0) / 1000.0,
				desc + "/sec", put_per_sec));
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

	public static void main(String[] args) throws RocksDBException {
		RocksDB.loadLibrary();
		RocksDB db = RocksDB.open(getDefaultOptions(true), "test-db");
		
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(true);

        WriteBatch writeBatch = new WriteBatch();
        int numBatch = 0;

		System.out.println("Starting " + NUM_TEST);
		long start = System.currentTimeMillis();

		Random rand = new Random();
		long seed = rand.nextLong();

		for (int i = 0; i < NUM_TEST; i++) {
			long keyVal = seed ^ i;

			byte[] key = Long.toString(keyVal).getBytes();
			byte[] val = Integer.toString(i).getBytes();
			writeBatch.put(key, val);
			numBatch++;
			if (numBatch >= 1000) {
				db.write(writeOptions, writeBatch);
				writeBatch.dispose();
				writeBatch = new WriteBatch();
				numBatch = 0;
			}

			if (i % 100000 == 0) {
				dumpStats(start, i, "Put");
			}
		}
		dumpStats(start, NUM_TEST, "Put");


		db.close();
	}

}
