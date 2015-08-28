package com.qualia.keystore_graph;


import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class GlobalKey {

    private static final HashFunction hashFunction = Hashing.murmur3_128();
    private static final byte CLEAR_BIT_MASK = (byte) 0xF0;
    private static final byte KEEP_BIT_MASK = (byte) 0x0F;
    public static final int KEY_LENGTH = 8;

    // hash is 8 bytes, 4 bits of 1st byte are used to store GlobalKeyType
    // Entropy is 60 bits of hash (+ a few bits of GlobalKeyType)
    // chance of collision is therefore 2^(60/2) ~= collision every 1 billion nodes (or better with entropy of
    // GlobalKeyType)
    private final byte[] hashValue;


    public static GlobalKey createFromPidUid(String pid, String uid) {
        byte[] srcBytes = hashFunction.hashString(pid + "_" + uid, Charsets.UTF_8).asBytes();
        byte[] hashValue = new byte[KEY_LENGTH];
        System.arraycopy(srcBytes, 0, hashValue, 0, KEY_LENGTH);
        GlobalKey result = new GlobalKey(hashValue);
        GlobalKeyType keyType = GlobalKeyType.fromPid(pid);
        embedKeyTypeInHash(hashValue, keyType);
        return result;
    }


    public static GlobalKey createFromDevice(DeviceTypes deviceType, String id) {
        byte[] hashValue = hashFunction.hashString(deviceType.toString() + "_" + id, Charsets.UTF_8).asBytes();
        GlobalKey result = new GlobalKey(hashValue);
        GlobalKeyType keyType = GlobalKeyType.DEVICE;
        embedKeyTypeInHash(hashValue, keyType);
        return result;
    }


    public static GlobalKey createFromBytes(byte[] bytes) {
        GlobalKey result = new GlobalKey(bytes);
        return result;
    }


    private static void embedKeyTypeInHash(byte[] hashValue, GlobalKeyType keyType) {
        byte b1 = hashValue[0];
        b1 &= CLEAR_BIT_MASK;
        Preconditions.checkState(keyType.ordinal() <= 15,
                "keyType ordinal can not be >15.  Would need to resize the bitmask");
        b1 |= (byte) keyType.ordinal();
        hashValue[0] = b1;
    }


    private GlobalKey(byte[] hashValue) {
        this.hashValue = hashValue;
    }


    public byte[] getHashValue() {
        return hashValue.clone(); // This is slow but ensures our copy never gets modified
    }


    public GlobalKeyType getGlobalKeyType() {
        byte b1 = (byte) (hashValue[0] & KEEP_BIT_MASK);
        return GlobalKeyType.values()[b1];
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(hashValue);
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GlobalKey other = (GlobalKey) obj;
        if (!Arrays.equals(hashValue, other.hashValue))
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "GlobalKey [" + getGlobalKeyType() + ", " + Arrays.toString(getHashValue()) + "]";
    }

}
