package com.qualia.keystore_graph;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Splitter;


public class KeyStoreAddThisMappingLoader extends FileLoader {

    private final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("YYYYmmdd");

    private Splitter tabSplitter = Splitter.on("\t");
    private Splitter commaSplitter = Splitter.on(",");
    private Splitter equalSplitter = Splitter.on("=");
    private HashMap<String, String> pidCodes = new HashMap<String, String>();


    public KeyStoreAddThisMappingLoader(Status status, File inputFile, String saveName) {
    	super(status, inputFile, saveName);
        pidCodes.put("6", "adnxs");
        pidCodes.put("9", "fat");
        pidCodes.put("11127", "ltm");
    }


	@Override
    public void processLine(String line, long curLine) {
        List<String> parts = tabSplitter.splitToList(line);
        String timestampStr = parts.get(0);
        DateTime dt = new DateTime(Long.valueOf(timestampStr));
        String dtStr = dateFmt.print(dt);
        Integer dtInt = Integer.valueOf(dtStr);
        // System.out.println(dt);
        String lr_uid = parts.get(1);
        String pidUidPairsList = parts.get(2);
        List<String> pidUidPairs = commaSplitter.splitToList(pidUidPairsList);

        ArrayList<GlobalKey> mappings = new ArrayList<GlobalKey>();

        GlobalKey lrKey = GlobalKey.createFromPidUid("lr", lr_uid);
        mappings.add(lrKey);
        storage.saveHashLookup(lrKey, "lr_" + lr_uid);
        storage.saveProperty(lrKey, PropertyLabel.LAST_SEEN, dtInt);

        for (String pidUidPair : pidUidPairs) {
            List<String> pidUidParts = equalSplitter.splitToList(pidUidPair);
            String pidCode = pidUidParts.get(0);
            String pid = pidCodes.get(pidCode);
            String uid = pidUidParts.get(1);
            if (uid.equals("0") || uid.equals("-1") || uid.equals("${profile_ID}")) {
                continue;
            }

            GlobalKey otherKey = GlobalKey.createFromPidUid(pid, uid);
            mappings.add(otherKey);
            storage.saveHashLookup(otherKey, pid + "_" + uid);
            storage.saveProperty(otherKey, PropertyLabel.LAST_SEEN, dtInt);
        }

        storage.saveMapping(mappings);
    }
    
}
