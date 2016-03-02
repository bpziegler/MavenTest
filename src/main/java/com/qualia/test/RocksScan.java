package com.qualia.test;

import java.io.UnsupportedEncodingException;

import com.qualia.keystore_graph.IScanCallback;
import com.qualia.keystore_graph.KeyStoreTable;

public class RocksScan {
    public static void main(String[] args) {
        String tableName = args[0];
        System.out.println("Scanning table:  " + tableName);

        KeyStoreTable table = new KeyStoreTable(tableName, false, true);

        table.scan(KeyStoreTable.getBytesForValue(""), new IScanCallback() {
            @Override
            public boolean onRow(byte[] key, byte[] value) {
                try {
                    String keyStr = new String(key, "UTF-8");
                    String valStr = new String(value, "UTF-8");
                    System.out.println(keyStr + ": " + valStr);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
        });

        table.close();
    }
}