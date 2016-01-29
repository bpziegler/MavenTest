package com.qualia.scoring;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Charsets;


public class MD5Helper {

    private final MessageDigest digest;


    public MD5Helper() {
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    public String stringToMD5Hex(String s) {
        byte[] bytes = s.getBytes(Charsets.UTF_8);
        byte[] digestBytes = digest.digest(bytes);
        return Hex.encodeHexString(digestBytes);
    }
}
