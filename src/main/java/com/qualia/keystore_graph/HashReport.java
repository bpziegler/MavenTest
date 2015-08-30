package com.qualia.keystore_graph;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.common.base.Charsets;
import com.localresponse.util.CountMap;

public class HashReport {

	public static void main(String[] args) throws RocksDBException {
        RocksDB db = RocksDB.openReadOnly("test-db/hash_lookup");
        
        RocksIterator iter = db.newIterator();
        iter.seekToFirst();
        
        GlobalKey prevRowKey = null;
        String prevId = "";
        long rowNum = 0;
        long numCollission = 0;
        long startTime = System.currentTimeMillis();
        CountMap pidCounts = new CountMap();
        
        while (iter.isValid()) {
        	byte[] key = iter.key();
        	byte[] tmp = new byte[GlobalKey.KEY_LENGTH];
        	System.arraycopy(key, 0, tmp, 0, GlobalKey.KEY_LENGTH);
        	GlobalKey rowKey = GlobalKey.createFromBytes(tmp);
        	String id = new String(key, GlobalKey.KEY_LENGTH, key.length - GlobalKey.KEY_LENGTH, Charsets.UTF_8);
        	
        	if (rowKey.equals(prevRowKey)) {
        		System.out.println(String.format("Hash collision   Key %s   id1 %s   id2 %s", rowKey, prevId, id));
        		numCollission++;
        	}
        	
        	if (rowKey.getGlobalKeyType().isCookieType()) {
	        	int underscoreIdx = id.indexOf("_");
	        	String pid = id.substring(0, underscoreIdx);
	        	pidCounts.updateCount(pid);
        	}
        	
        	prevRowKey = rowKey;
        	prevId = id;
        	
        	if (rowNum % 50000 == 0) {
        		double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000.0;
        		double rowPerSec = rowNum / elap;
        		String s = String.format("Elap %,8.1f   Row %,10d   Row/Sec %,8.0f   Collisions %,5d   %s", elap, rowNum, rowPerSec, numCollission, id);
        		System.out.println(s);
        	}
        	
        	iter.next();
        	rowNum++;
        }
        
        System.out.println(pidCounts.report());
		
        db.close();
        db.dispose();
	}

}
