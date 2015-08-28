package com.qualia.keystore_graph;


public interface IScanCallback {
    public boolean onRow(byte[] key, byte[] value);
}
