package com.localresponse.neo4j_tool;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class TestNeo4jEmbedded {

	public static void createTestNodes(GraphDatabaseService graphDb) {
		Transaction tx = graphDb.beginTx();

		long startTime = System.currentTimeMillis();

		System.out.println("Creating nodes");
		for (int i = 0; i < 1000; i++) {
			Node firstNode = graphDb.createNode();
			firstNode.setProperty("create_id", i);
		}

		long elap = System.currentTimeMillis() - startTime;
		System.out.println("Elap = " + elap);

		tx.success();
		tx.close();
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Starting up");

		String filename = "/Users/benziegler/work/neo4j-community-2.1.2/data/graph.db/";
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(filename);
		
		FileOutputStream fs = new FileOutputStream("graph_dump.json");
		OutputStreamWriter osw = new OutputStreamWriter(fs);
		BufferedWriter bw = new BufferedWriter(osw, 256 * 1024);
		PrintWriter pw = new PrintWriter(bw, false);
		
		// createTestNodes(graphDb);
		
		GraphToJsonDumper graphDumper = new GraphToJsonDumper(graphDb, pw);
		graphDumper.exportDump();
		
		System.out.println("Shutting down");
		graphDb.shutdown();
		System.out.println("Finished");
	}

}
