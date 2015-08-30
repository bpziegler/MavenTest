package com.qualia.keystore_graph;

import org.rocksdb.RocksDBException;

public class DumpClusters {

	public static void main(String[] args) throws RocksDBException, InterruptedException {
		
		ClusterThreadMgr mgr = new ClusterThreadMgr();
		mgr.run();
	}

}
