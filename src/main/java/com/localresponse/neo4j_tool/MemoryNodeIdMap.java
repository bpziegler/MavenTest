package com.localresponse.neo4j_tool;


import gnu.trove.map.hash.TLongLongHashMap;


public class MemoryNodeIdMap implements INodeIdMap {

    private final TLongLongHashMap memoryMap = new TLongLongHashMap();


    @Override
    public void put(long oldId, long newId) {
        memoryMap.put(oldId, newId);
    }


    @Override
    public long get(long oldId) {
        return memoryMap.get(oldId);
    }

}
