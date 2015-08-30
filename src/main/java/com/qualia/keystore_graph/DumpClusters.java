package com.qualia.keystore_graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class DumpClusters {

	private static void dumpCluster(GraphStorage storage, Collection<GlobalKey> cluster) {
		List<String> ids = new ArrayList<String>();
		for (GlobalKey key : cluster) {
			String id = storage.lookupId(key);
			ids.add(id);
		}
		Collections.sort(ids);
		int i = 0;
		for (String id : ids) {
			i++;
			System.out.println(String.format("%4d of %4d   %s", i, cluster.size(), id));
		}
	}

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
		Collection<GlobalKey> maxCluster = null;

		while (iter.isValid()) {
			byte[] key = iter.key();
			byte[] tmp = new byte[GlobalKey.KEY_LENGTH];
			System.arraycopy(key, 0, tmp, 0, GlobalKey.KEY_LENGTH);
			GlobalKey rowKey = GlobalKey.createFromBytes(tmp);

			if (visited.contains(rowKey.getHashValueAsLong())) {
				// Skip
			} else {
				numCluster++;
				Collection<GlobalKey> keys = storage.getAllMappings(rowKey, 100000);
				for (GlobalKey oneKey : keys) {
					visited.add(oneKey.getHashValueAsLong());
				}
				if (maxCluster == null || keys.size() > maxCluster.size()) {
					maxCluster = keys;
					dumpCluster(storage, maxCluster);
				}
			}

			long now = System.currentTimeMillis();
			if (now - lastLog >= 250) {
				lastLog = now;
				double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000.0;
				double rowPerSec = rowNum / elap;
				int maxClusterSize = (maxCluster != null) ? maxCluster.size() : -1;
				String s = String.format("Elap %,8.1f   Row %,10d   Row/Sec %,8.0f   Clusters %,8d   Visited %,8d   MaxClusSize %,4d", elap, rowNum, rowPerSec, numCluster, visited.size(), maxClusterSize);
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
