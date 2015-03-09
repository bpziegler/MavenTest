package com.localresponse.neo4j_tool;


import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import com.google.common.base.Charsets;


public class GraphUniquePropertyChecker {

    private final String srcGraphDir;
    private final GraphDatabaseService srcDb;
    private final List<String> uniqueProperties = Arrays.asList("guid", "ownza_id", "ip_address", "pid_uid",
            "bc_household_id", "idfa", "android_ad_id", "android_id", "android_id_sha1", "android_id_md5");
    private final HashMap<String, OutputStreamWriter> streamMap = new HashMap<String, OutputStreamWriter>();


    public GraphUniquePropertyChecker(String srcGraphDir) {
        this.srcGraphDir = srcGraphDir;
        this.srcDb = new GraphDatabaseFactory().newEmbeddedDatabase(srcGraphDir);
    }


    private void run() throws IOException {
        Transaction tx = srcDb.beginTx();

        long numNode = checkProperties();
        System.out.println("Num Nodes copied = " + numNode);

        tx.success();
        tx.close();

        System.out.println("Shutting down srcDb");
        srcDb.shutdown();
    }


    private long checkProperties() throws IOException {
        System.out.println("Checking properties");

        long startTime = System.currentTimeMillis();
        long lastLog = 0;

        GlobalGraphOperations globalOps = GlobalGraphOperations.at(srcDb);

        Iterable<Node> iter = globalOps.getAllNodes();

        long maxId = 0;

        int num = 0;
        for (Node oneNode : iter) {
            long nodeId = oneNode.getId();
            Map<String, Object> props = getProperties(oneNode);
            // System.out.println(nodeId + "  " + props);

            for (String propName : props.keySet()) {
                if (uniqueProperties.indexOf(propName) != -1) {
                    Object val = props.get(propName);
                    logPropertyValue(propName, val, nodeId);
                }
            }

            num++;
            if (System.currentTimeMillis() - lastLog >= 250) {
                lastLog = printStatus("Nodes", startTime, num);
            }
        }

        System.out.println(String.format("MaxId = %,10d   NumId = %,10d", maxId, num));

        for (OutputStreamWriter osw : streamMap.values()) {
            osw.close();
        }

        return num;
    }


    private void logPropertyValue(String propName, Object val, long nodeId) throws IOException {
        OutputStreamWriter osw = streamMap.get(propName);
        if (osw == null) {
            String path = propName + ".txt";
            System.out.println("Creating log file:  " + path);
            FileOutputStream fs = new FileOutputStream(path);
            BufferedOutputStream bf = new BufferedOutputStream(fs);
            osw = new OutputStreamWriter(bf, Charsets.UTF_8);
            streamMap.put(propName, osw);
        }

        osw.write(val + "\t" + nodeId + "\n");
    }


    private long printStatus(String desc, long startTime, int num) {
        double elapSec = (System.currentTimeMillis() - startTime + 0.0) / 1000;
        double opsPerSec = num / elapSec;
        System.out.println(String.format("%-12s   Line %,12d   Elap %,7.1f   Line/Sec %,7.0f", desc, num, elapSec,
                opsPerSec));
        return System.currentTimeMillis();
    }


    private static Map<String, Object> getProperties(PropertyContainer pc) {
        Map<String, Object> result = new HashMap<String, Object>();
        for (String property : pc.getPropertyKeys()) {
            result.put(property, pc.getProperty(property));
        }
        return result;
    }


    public static void main(String[] args) throws IOException {
        String srcDir = "/Users/benziegler/work/neo4j-community-2.1.2/data/graph.db";

        if (args.length > 0) {
            srcDir = args[0];
            System.out.println("srcDir = " + srcDir);
        }

        GraphUniquePropertyChecker program = new GraphUniquePropertyChecker(srcDir);
        program.run();
    }

}
