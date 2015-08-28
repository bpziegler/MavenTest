package com.qualia.keystore_graph;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.rocksdb.RocksDBException;

import com.google.common.base.Charsets;


public class GraphStorage {

    private final KeyStoreTable mappingTable = new KeyStoreTable("mapping", false);
    private final KeyStoreTable propertyTable = new KeyStoreTable("property", true);
    private final KeyStoreTable hashLookupTable = new KeyStoreTable("hash_lookup", true);
    private final KeyStoreTable ipMappingTable = new KeyStoreTable("ip_mapping", true);


    public GraphStorage() {

    }


    public void saveMapping(List<GlobalKey> globalKeys) {
        // Pick one at random, and then map that one to each other key directly
        // We pick one at random to ensure an even keyspace
        List<GlobalKey> copy = new ArrayList<GlobalKey>(globalKeys);
        Collections.shuffle(copy);

        GlobalKey first = copy.remove(0);
        for (GlobalKey oneOther : copy) {
            saveMappingPair(first, oneOther);
            saveMappingPair(oneOther, first);
        }
    }


    public void saveProperty(GlobalKey globalKey, PropertyLabel propLabel, Object value) {
        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            bs.write(globalKey.getHashValue());
            bs.write(propLabel.ordinal());

            propertyTable.put(bs.toByteArray(), value);
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    public Map<PropertyLabel, Object> getProperties(GlobalKey globalKey) {
        return null;
    }


    public void saveIPMapping(GlobalKey globalKey, int packedIPAddress, int lastSeen) {

    }


    public void saveHashLookup(GlobalKey globalKey, String origId) {
        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            bs.write(globalKey.getHashValue());
            bs.write(origId.getBytes(Charsets.UTF_8));

            hashLookupTable.put(bs.toByteArray(), "");
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


    public void close() {
        mappingTable.close();
        propertyTable.close();
        hashLookupTable.close();
        ipMappingTable.close();
    }


    private void saveMappingPair(GlobalKey key1, GlobalKey key2) {
        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            bs.write(key1.getHashValue());
            bs.write(key2.getHashValue());

            mappingTable.put(bs.toByteArray(), "");
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
