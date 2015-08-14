package com.localresponse.neo4j_tool;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DumpStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;


public class Neo4jStoreStats extends DumpStore<PropertyRecord, PropertyStore> {

    protected Neo4jStoreStats(PrintStream out) {
        super(out);
    }


    private static StringLogger logger() {
        return StringLogger.SYSTEM;
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            // Do Nothing
        }
    }


    public static void main(String[] args) throws Exception {
        String dir = "//Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";

        File file = null;
        if (args.length > 0) {
            file = new File(args[0]);
        } else {
            file = new File(dir, "neostore.propertystore.db");
        }
        System.out.println("Using file:  " + file.getAbsolutePath());

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), logger(), null);
        PropertyStore store = storeFactory.newPropertyStore(file);
        try {
            PrintStream ps = new PrintStream(new NullOutputStream());
            Neo4jStoreStats dumpStore = new Neo4jStoreStats(ps) {
                private long numUsed;
                private long numDeleted;
                private long numRec;
                private long lastLog = 0;
                private long start = System.currentTimeMillis();


                @Override
                protected Object transform(PropertyRecord record) throws Exception {
                    numRec++;
                    if (record.inUse()) {
                        numUsed++;
                    } else {
                        numDeleted++;
                    }

                    long now = System.currentTimeMillis();
                    if (now - lastLog >= 250) {
                        lastLog = now;
                        dumpStats();
                    }

                    return "";
                }


                protected void dumpStats() {
                    double elap = (0.0 + System.currentTimeMillis() - start) / 1000;
                    double lines_per = numRec / elap;
                    String status = String.format("Elap %,9.1f   Tot/sec %,6.0f   Tot %,d   Used %,d   Deleted %,d", elap, lines_per, numRec, numUsed, numDeleted);
                    System.out.println(status);
                }
            };
            System.out.println("Begin dump");
            dumpStore.dump(store);
        } finally {
            store.close();
        }
    }

}
