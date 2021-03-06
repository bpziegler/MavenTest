package com.qualia.keystore_graph;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;

import org.parboiled.common.Preconditions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class ClusterThreadMgr extends PoolWorkerManager<GraphStorage, GlobalKey, MappingResult> {

	private final TLongSet visited = new TLongHashSet();
	private final GraphStorage storage = new GraphStorage(true);
	private MappingResult maxCluster = null;
    private BufferedWriter largeClusterStreamWriter;

	@Override
	public GraphStorage createContext() {
		return new GraphStorage(true);
	}

	@Override
	public MappingResult transformInThread(GraphStorage storage, GlobalKey rowKey) {
	    MappingResult result;
		try {
			Preconditions.checkArgNotNull(rowKey, "rowKey can't be null");
			result = storage.getAllMappings(rowKey, 1000);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return result;
	}

	@Override
	public void consume(MappingResult mappingResult) {
		for (GlobalKey oneKey : mappingResult.allKeys) {
			visited.add(oneKey.getHashValueAsLong());
		}
		//  Note we are doing ">=" so we get all clusters that are of size 1000
		if (maxCluster == null || mappingResult.allKeys.size() >= maxCluster.allKeys.size()) {
			maxCluster = mappingResult;
			// storage.dumpCluster(maxCluster);
			GlobalKey mostConnectedKey = mappingResult.getMostConnectedKey();
			int oneKeySize = mappingResult.directMappings.get(mostConnectedKey).size();
			String id = storage.lookupId(mostConnectedKey);
			String line = String.format("%d\t%s\t%d", maxCluster.allKeys.size(), id, oneKeySize);
			try {
                largeClusterStreamWriter.write(line);
                largeClusterStreamWriter.newLine();
                largeClusterStreamWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
		}
	}

	public void run() throws InterruptedException, RocksDBException, IOException {
		start(5);
		
		FileOutputStream fs = new FileOutputStream("large_clusters.log");
		OutputStreamWriter osw = new OutputStreamWriter(fs);
		largeClusterStreamWriter = new BufferedWriter(osw);

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

			if (visited.contains(rowKey.getHashValueAsLong())) {
				// Skip
			} else {
				numCluster++;
				addInput(rowKey);
			}

			long now = System.currentTimeMillis();
			if (now - lastLog >= 250) {
				lastLog = now;
				dumpStats(rowNum, startTime, numCluster);
			}

			iter.next();
			rowNum++;
		}

		stop();
		dumpStats(rowNum, startTime, numCluster);

		storage.close();
		db.close();
		db.dispose();
		largeClusterStreamWriter.close();
	}

	private void dumpStats(long rowNum, long startTime, long numCluster) {
		double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000.0;
		double rowPerSec = rowNum / elap;
		int maxClusterSize = (maxCluster != null) ? maxCluster.allKeys.size() : -1;
		String s = String.format(
				"Elap %,8.1f   Row %,10d   Row/Sec %,8.0f   Clusters %,8d   Visited %,8d   MaxClusSize %,4d", elap,
				rowNum, rowPerSec, numCluster, visited.size(), maxClusterSize);
		System.out.println(s);
	}

}
