package com.qualia.keystore_graph;

import java.util.Collection;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class DumpClusters {

	public static void main(String[] args) throws RocksDBException {
		TLongSet visited = new TLongHashSet();

		GraphStorage storage = new GraphStorage(true);
		RocksDB db = RocksDB.openReadOnly("test-db/hash_lookup");

		RocksIterator iter = db.newIterator();
		iter.seekToFirst();

		long rowNum = 0;
		long startTime = System.currentTimeMillis();
		long lastLog = 0;
		long numCluster = 0;

		while (iter.isValid()) {
			byte[] key = iter.key();
			byte[] tmp = new byte[GlobalKey.KEY_LENGTH];
			System.arraycopy(key, 0, tmp, 0, GlobalKey.KEY_LENGTH);
			GlobalKey rowKey = GlobalKey.createFromBytes(tmp);
			String id = new String(key, GlobalKey.KEY_LENGTH, key.length - GlobalKey.KEY_LENGTH);

			if (visited.contains(rowKey.getHashValueAsLong())) {
				// Skip
			} else {
				numCluster++;
				Collection<GlobalKey> keys = storage.getAllMappings(rowKey);
				for (GlobalKey oneKey : keys) {
					visited.add(oneKey.getHashValueAsLong());
				}
			}

			long now = System.currentTimeMillis();
			if (now - lastLog >= 250) {
				lastLog = now;
				double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000.0;
				double rowPerSec = rowNum / elap;
				String s = String.format("Elap %,8.1f   Row %,10d   Row/Sec %,8.0f   Clusters %,8d   Visited %,8d", elap, rowNum, rowPerSec, numCluster, visited.size());
				System.out.println(s);
			}

			iter.next();
			rowNum++;
		}

		storage.close();
		db.close();
		db.dispose();
	}

}
