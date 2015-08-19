package org.neo4j.kernel.impl.nioneo.store;


import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;


public class LabelDumper {

    public static Map<Integer, String> getLabelMap(File file) {
        Map<Integer, String> map = new HashMap<Integer, String>();

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, null);
        LabelTokenStore store = storeFactory.newLabelTokenStore(file);

        store.makeStoreOk();
        int size = store.getRecordSize();

        long maxId = store.getHighestPossibleIdInUse();
        for (long i = 0; i <= maxId; i++) {
            LabelTokenRecord record = store.forceGetRecord(i);
            if (record.inUse()) {
                store.ensureHeavy(record);
                String str = store.getStringFor(record);
                map.put(record.getId(), str);
            }
        }

        return map;
    }


    public static void main(String[] args) {
        String dir = "//Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";
        File file = new File(dir, "neostore.labeltokenstore.db");
        LabelDumper program = new LabelDumper();
        program.getLabelMap(file);
    }

}
