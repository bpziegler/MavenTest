package com.qualia.keystore_graph;


import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class GlobalKey {

    private static final HashFunction hashFunction = Hashing.murmur3_128();
    private static final byte BIT_MASK = 0xF;

    // hash is 8 bytes, 4 bits of 1st byte are used to store GlobalKeyType
    // Entropy is 60 bits of hash (+ a few bits of GlobalKeyType)
    // chance of collision is therefore 2^(60/2) ~= collision every 1 billion nodes (or better with entropy of GlobalKeyType)
    private final byte[] hashValue; 


    public static GlobalKey createFromPidUid(String pid, String uid) {
        byte[] hashValue = hashFunction.hashString(pid + "_" + uid, Charsets.UTF_8).asBytes();
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


    private static void embedKeyTypeInHash(byte[] hashValue, GlobalKeyType keyType) {
        byte b1 = hashValue[0];
        b1 &= BIT_MASK;
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
        byte b1 = (byte) (hashValue[0] & BIT_MASK);
        return GlobalKeyType.values()[b1];
    }

}
