package com.localresponse.neo4j_tool;


/*
 * Portions from:
 * https://groups.google.com/forum/#!searchin/neo4j/compact/neo4j/rgYVs64Xc8I/FZBrCKsdlygJ
 * 
 */

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;


public class GraphCompactor {

    private final String srcGraphDir;
    private final String destGraphDir;
    private final GraphDatabaseService srcDb;
    private final BatchInserter targetDb;
    private final TLongLongHashMap nodeIdMap = new TLongLongHashMap(); // Key is the Old ID, value is the New ID

    private static final List<String> propsToRemove = Arrays.asList("created_on", "last_seen");

    private long nextNodeId = 0;


    public GraphCompactor(String srcGraphDir, String destGraphDir) {
        this.srcGraphDir = srcGraphDir;
        this.destGraphDir = destGraphDir;
        this.srcDb = new GraphDatabaseFactory().newEmbeddedDatabase(srcGraphDir);
        this.targetDb = BatchInserters.inserter(destGraphDir);
    }


    public void compact() throws IOException {
        Transaction tx = srcDb.beginTx();

        int numNode = copyNodes();
        System.out.println("Num Nodes copied = " + numNode);

        int numRel = copyRelationships();
        System.out.println("Num Nodes copied = " + numNode);
        System.out.println("Num Relationships copied = " + numRel);

        tx.success();
        tx.close();

        System.out.println("Shutting down srcDb");
        srcDb.shutdown();
        System.out.println("Shutting down targetDb");
        targetDb.shutdown();
    }


    private int copyNodes() {
        System.out.println("Copying nodes");

        long startTime = System.currentTimeMillis();
        long lastLog = 0;

        GlobalGraphOperations globalOps = GlobalGraphOperations.at(srcDb);

        Iterable<Node> iter = globalOps.getAllNodes();

        long maxId = 0;

        int num = 0;
        for (Node oneNode : iter) {
            // Map IDs into new packed IDs
            long oldId = oneNode.getId();
            long newId = nextNodeId++;
            nodeIdMap.put(oldId, newId);

            List<Label> labelList = new ArrayList<Label>();
            Iterator<Label> labelIter = oneNode.getLabels().iterator();
            while (labelIter.hasNext()) {
                Label oneLabel = labelIter.next();
                labelList.add(oneLabel);
            }

            Map<String, Object> props = getProperties(oneNode);
            filterProps(props);
            targetDb.createNode(newId, props, (Label[]) labelList.toArray(new Label[] {}));

            if (oneNode.getId() > maxId) {
                maxId = oneNode.getId();
            }

            num++;
            if (System.currentTimeMillis() - lastLog >= 250) {
                lastLog = printStatus("Nodes", startTime, num);
            }
        }

        System.out.println(String.format("MaxId = %,10d   NumId = %,10d", maxId, num));

        return num;
    }


    private void filterProps(Map<String, Object> props) {
        Iterator<String> iter = props.keySet().iterator();
        while (iter.hasNext()) {
            String oneKey = iter.next();
            if (propsToRemove.contains(oneKey)) {
                iter.remove();
            }
        }
    }


    private int copyRelationships() {
        long startTime = System.currentTimeMillis();
        long lastLog = 0;

        int num = 0;

        GlobalGraphOperations globalOps = GlobalGraphOperations.at(srcDb);
        Iterable<Node> iter = globalOps.getAllNodes();

        for (Node node : iter) {
            for (Relationship rel : getOutgoingRelationships(node)) {
                createRelationship(rel);

                num++;
                if (System.currentTimeMillis() - lastLog >= 250) {
                    lastLog = printStatus("Rel", startTime, num);
                }
            }
        }

        return num;
    }


    private void createRelationship(Relationship rel) {
        long startNodeId = rel.getStartNode().getId();
        long endNodeId = rel.getEndNode().getId();

        long newStartNode = nodeIdMap.get(startNodeId);
        long newEndNode = nodeIdMap.get(endNodeId);

        final RelationshipType type = rel.getType();
        Map<String, Object> props = getProperties(rel);
        // System.out.println("createRelationship:  " + type.toString() + "   " + props.toString());
        targetDb.createRelationship(newStartNode, newEndNode, type, props);
    }


    private static Iterable<Relationship> getOutgoingRelationships(Node node) {
        return node.getRelationships(Direction.OUTGOING);
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


    public String getSrcGraphDir() {
        return srcGraphDir;
    }


    public String getDestGraphDir() {
        return destGraphDir;
    }


    public static void main(String[] args) throws Exception {
        final String DEFAULT_SRC_DIR = "/Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";
        final String DEFAULT_DEST_DIR = "dest-graph";

        Options options = new Options();
        options.addOption("s", "srcdir", true, "Directory to source graph (graph.db)");
        options.addOption("d", "destdir", true, "Directory for destination graph");
        options.addOption("h", "help", false, "Print usage help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("GraphCompactor", options);
            return;
        }

        String srcDir = cmd.getOptionValue("srcdir", DEFAULT_SRC_DIR);
        String destDir = cmd.getOptionValue("destdir", DEFAULT_DEST_DIR);

        System.out.println("srcDir = " + srcDir);
        System.out.println("destDir = " + destDir);

        GraphCompactor compactor = new GraphCompactor(srcDir, destDir);
        compactor.compact();
    }
}
