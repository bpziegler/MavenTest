package org.neo4j.kernel.impl.nioneo.store;


import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;


public class RelationshipTypeDumper {

    public static Map<Integer, String> getTypeMap(File file) {
        Map<Integer, String> map = new HashMap<Integer, String>();

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, null);
        RelationshipTypeTokenStore store = storeFactory.newRelationshipTypeTokenStore(file);

        store.makeStoreOk();
        int size = store.getRecordSize();

        long maxId = store.getHighestPossibleIdInUse();
        for (long i = 0; i <= maxId; i++) {
            RelationshipTypeTokenRecord record = store.forceGetRecord(i);
            if (record.inUse()) {
                store.ensureHeavy(record);
                String str = store.getStringFor(record);
                map.put(record.getId(), str);
            }
        }

        store.close();

        return map;
    }


    public static void main(String[] args) {
        String dir = "//Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";
        File file = new File(dir, "neostore.relationshiptypestore.db");
        Map<Integer, String> result = RelationshipTypeDumper.getTypeMap(file);
        System.out.println("types = " + result.toString());
    }

}
