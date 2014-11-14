package com.localresponse.neo4j_tool;


/*
 * Portions from:
 * https://groups.google.com/forum/#!searchin/neo4j/compact/neo4j/rgYVs64Xc8I/FZBrCKsdlygJ
 * 
 */

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
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
			// TODO: Map IDs into new packed IDs
			targetDb.createNode(oneNode.getId(), getProperties(oneNode));
			
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
		final RelationshipType type = rel.getType();
		targetDb.createRelationship(startNodeId, endNodeId, type, getProperties(rel));
	}


	private static Iterable<Relationship> getOutgoingRelationships(Node node) {
		return node.getRelationships(Direction.OUTGOING);
	}


	private long printStatus(String desc, long startTime, int num) {
		double elapSec = (System.currentTimeMillis() - startTime + 0.0) / 1000;
		double opsPerSec = num / elapSec;
		System.out.println(String.format("%-12s   Line %,12d   Elap %,7.1f   Line/Sec %,7.0f", desc, num, elapSec, opsPerSec));
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
		String srcDir = "/Users/benziegler/work/neo4j-community-2.1.2/data/graph.db";
		
        if (args.length > 0) {
            srcDir = args[0];
            System.out.println("srcDir = " + srcDir);
        }
		
		Path destDir = Files.createTempDirectory("temp-graph-db", new FileAttribute[] {});
		String destDirStr = destDir.toFile().getAbsolutePath();

		System.out.println("DestDir = " + destDirStr);

		GraphCompactor compactor = new GraphCompactor(srcDir, destDirStr);
		compactor.compact();
	}
}
