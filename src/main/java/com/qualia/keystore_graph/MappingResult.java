package com.qualia.keystore_graph;


import java.util.Collection;
import java.util.Map;


public class MappingResult {
    public final Collection<GlobalKey> allKeys;
    public final Map<GlobalKey, Collection<GlobalKey>> directMappings;


    public MappingResult(Collection<GlobalKey> allKeys, Map<GlobalKey, Collection<GlobalKey>> directMappings) {
        super();
        this.allKeys = allKeys;
        this.directMappings = directMappings;
    }


    public GlobalKey getMostConnectedKey() {
        GlobalKey result = null;
        int largestSize = -1;

        for (GlobalKey key : directMappings.keySet()) {
            Collection<GlobalKey> val = directMappings.get(key);
            if (val.size() > largestSize) {
                result = key;
                largestSize = val.size();
            }
        }

        return result;
    }
}
