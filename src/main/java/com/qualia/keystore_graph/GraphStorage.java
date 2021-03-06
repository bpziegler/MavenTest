package com.qualia.keystore_graph;


import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rocksdb.RocksDBException;

import com.google.common.base.Charsets;


public class GraphStorage implements Closeable {

    private static final int MAX_MAPPINGS_PER_KEY = 32;

    private final KeyStoreTable mappingTable;
    private final KeyStoreTable propertyTable;
    private final KeyStoreTable hashLookupTable;
    private final KeyStoreTable ipMappingTable;
    private final KeyStoreTable fileSaveTable;


    public GraphStorage(boolean readOnly) {
        mappingTable = new KeyStoreTable("mapping", false, readOnly);
        propertyTable = new KeyStoreTable("property", true, readOnly);
        hashLookupTable = new KeyStoreTable("hash_lookup", true, readOnly);
        ipMappingTable = new KeyStoreTable("ip_mapping", true, readOnly);
        fileSaveTable = new KeyStoreTable("file_save", true, readOnly);
        // We want this to update immediately. Its low volume so its OK.
        fileSaveTable.setWriteToWAL(true);
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


    public MappingResult getAllMappings(GlobalKey rootKey) {
        return getAllMappings(rootKey, MAX_MAPPINGS_PER_KEY);
    }


    public MappingResult getAllMappings(GlobalKey rootKey, int maxMappings) {
        Map<GlobalKey, Collection<GlobalKey>> allDirectMappings = new HashMap<GlobalKey, Collection<GlobalKey>>();

        Set<GlobalKey> followedKeys = new HashSet<GlobalKey>();
        Set<GlobalKey> unFollowedKeys = new HashSet<GlobalKey>();
        unFollowedKeys.add(rootKey);

        while (unFollowedKeys.size() > 0) {
            GlobalKey srcKey = unFollowedKeys.iterator().next();
            List<GlobalKey> directMappings = getDirectMappings(srcKey, maxMappings);
            allDirectMappings.put(srcKey, directMappings);
            unFollowedKeys.remove(srcKey);
            followedKeys.add(srcKey);
            for (GlobalKey foundKey : directMappings) {
                if (!followedKeys.contains(foundKey)) {
                    unFollowedKeys.add(foundKey);
                }
            }
            if (followedKeys.size() >= maxMappings) {
                break;
            }
        }

        return new MappingResult(followedKeys, allDirectMappings);
    }


    private List<GlobalKey> getDirectMappings(final GlobalKey srcKey, final int maxMappings) {
        final List<GlobalKey> result = new ArrayList<GlobalKey>();
        final byte[] srcHash = srcKey.getHashValue();

        // Scan where the srcKey bytes match the first bytes from the scan key.
        // Key of the Mapping table is [src.bytes][dest.bytes]
        mappingTable.scan(srcHash, new IScanCallback() {
            private int num = 0;


            @Override
            public boolean onRow(byte[] key, byte[] value) {
                boolean keyMatches = true;
                for (int i = 0; i < srcHash.length; i++) {
                    if (srcHash[i] != key[i]) {
                        keyMatches = false;
                        break;
                    }
                }
                if (keyMatches) {
                    byte[] tmp = new byte[GlobalKey.KEY_LENGTH];
                    System.arraycopy(key, srcHash.length, tmp, 0, GlobalKey.KEY_LENGTH);
                    GlobalKey destKey = GlobalKey.createFromBytes(tmp);
                    result.add(destKey);
                }
                num++;
                if (num >= maxMappings) {
                    return false;
                }
                return keyMatches;
            }
        });

        return result;
    }


    public Map<PropertyLabel, Object> getProperties(GlobalKey globalKey) {
        return null;
    }


    public void saveIPMapping(GlobalKey globalKey, int packedIPAddress, int lastSeen) {
        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            bs.write(globalKey.getHashValue());
            bs.write(KeyStoreTable.getBytesForValue(packedIPAddress));
            // Note - we could also include lastSeen in the key. Would take
            // more storage, but allow us to get a count.

            ipMappingTable.put(bs.toByteArray(), KeyStoreTable.getBytesForValue(lastSeen));
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
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


    public void saveLoadFileProperty(String loadedFileName, String propName, Object value) {
        try {
            String key = loadedFileName + "\t" + propName;
            fileSaveTable.putNoBatch(key.getBytes(Charsets.UTF_8), value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


    public String lookupId(final GlobalKey globalKey) {
        final List<String> ids = new ArrayList<String>();

        hashLookupTable.scan(globalKey.getHashValue(), new IScanCallback() {
            @Override
            public boolean onRow(byte[] key, byte[] value) {
                byte[] tmp = new byte[GlobalKey.KEY_LENGTH];
                System.arraycopy(key, 0, tmp, 0, GlobalKey.KEY_LENGTH);
                GlobalKey destKey = GlobalKey.createFromBytes(tmp);

                if (destKey.equals(globalKey)) {
                    String id = new String(key, GlobalKey.KEY_LENGTH, key.length - GlobalKey.KEY_LENGTH, Charsets.UTF_8);
                    ids.add(id);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // Should we even bother reporting if ids.size > 1?

        String result = (ids.size() > 0) ? ids.get(0) : null;

        return result;
    }


    public void close() {
        mappingTable.close();
        propertyTable.close();
        hashLookupTable.close();
        ipMappingTable.close();
        fileSaveTable.close();
    }


    public void dumpCluster(Collection<GlobalKey> cluster) {
        List<String> ids = new ArrayList<String>();
        for (GlobalKey key : cluster) {
            String id = this.lookupId(key);
            ids.add(id);
        }
        Collections.sort(ids);
        int i = 0;
        for (String id : ids) {
            i++;
            System.out.println(String.format("%4d of %4d   %s", i, cluster.size(), id));
        }
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
