package com.qualia.keystore_graph;


import java.util.Collection;


public class GraphStorageTest {

    public static void main(String[] args) {
        String lr_uid = (args.length > 0) ? args[0] : "00000e29-7a9f-4ec2-beb2-6c86c518c973";

        GlobalKey rootKey = GlobalKey.createFromPidUid("lr", lr_uid);
        System.out.println("RootKey = " + rootKey);

        GraphStorage storage = new GraphStorage();
        Collection<GlobalKey> keys = storage.getAllMappings(rootKey);
        for (GlobalKey oneKey : keys) {
            System.out.println(oneKey);
        }
    }

}
