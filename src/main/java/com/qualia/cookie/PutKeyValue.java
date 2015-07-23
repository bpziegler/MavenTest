package com.qualia.cookie;


public class PutKeyValue {

    private final byte[] key;
    private final byte[] val;


    public PutKeyValue(byte[] key, byte[] val) {
        super();
        this.key = key;
        this.val = val;
    }


    public byte[] getKey() {
        return key;
    }


    public byte[] getVal() {
        return val;
    }

}
