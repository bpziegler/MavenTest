package com.localresponse.neo4j_tool;


public interface INodeIdMap {
    public void put(long oldId, long newId);
    public long get(long oldId);
}
