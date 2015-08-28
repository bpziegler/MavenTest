package com.qualia.keystore_graph;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class GraphStorageTest {

    public static void main(String[] args) throws IOException {
        List<String> pid_uids = new ArrayList<String>();

        if (args.length > 0) {
            List<String> lines = Files.readLines(new File(args[0]), Charsets.UTF_8);
            for (String line : lines) {
                pid_uids.add("lr_" + line);
            }
        } else {
            String pid_uid = (args.length > 0) ? args[0] : "lr_00000e29-7a9f-4ec2-beb2-6c86c518c973";
            pid_uids.add(pid_uid);
        }

        GraphStorage storage = new GraphStorage(true);
        int num = 0;
        for (String pid_uid : pid_uids) {
            int underscore = pid_uid.indexOf("_");
            String pid = pid_uid.substring(0, underscore);
            String uid = pid_uid.substring(underscore + 1);
            GlobalKey rootKey = GlobalKey.createFromPidUid(pid, uid);
            Collection<GlobalKey> keys = storage.getAllMappings(rootKey);

            if (num % 100 == 0) {
                System.out.println("Num = " + num + "   " + keys.toString());
            }
            num++;
        }

        storage.close();
    }
}
