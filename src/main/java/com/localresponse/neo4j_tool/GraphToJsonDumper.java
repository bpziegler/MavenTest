package com.localresponse.neo4j_tool;

import java.io.IOException;
import java.io.PrintWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class GraphToJsonDumper {
	
	private final GraphDatabaseService graphDb;
	private final ObjectMapper mapper = new ObjectMapper();
	private final PrintWriter printWriter;

	public GraphToJsonDumper(GraphDatabaseService graphDb, PrintWriter printWriter)
	{
		this.graphDb = graphDb;
		this.printWriter = printWriter;
	}
	
	public void exportDump() throws IOException {
		long startTime = System.currentTimeMillis();
		long lastLog = 0;
		
		Transaction tx = graphDb.beginTx();

		System.out.println("Iterating nodes");
		Iterable<Node> iter = graphDb.getAllNodes();

		int num = 0;
		for (Node oneNode : iter) {
			Iterable<Relationship> relationships = oneNode.getRelationships(Direction.OUTGOING);
			oneNode.getPropertyKeys();

			
			// dumpOneNode(oneNode);
			num++;
			
			if (System.currentTimeMillis() - lastLog >= 250) {
				lastLog = System.currentTimeMillis();
				double elapSec = (System.currentTimeMillis() - startTime + 0.0) / 1000;
				double opsPerSec = num / elapSec;
				System.out.println(String.format("Line %,12d   Elap %,7.1f   Line/Sec %,7.0f", num, elapSec, opsPerSec));
			}
		}
		System.out.println("Num Node = " + num);
		printWriter.close();

		tx.success();
		tx.close();
	}

	private void dumpOneNode(Node oneNode) {
		ObjectNode jsonNode = mapper.createObjectNode();
		jsonNode.put("id", oneNode.getId());
		
		ObjectNode nodeProps = copyPropertiesToJson(oneNode);
		if (nodeProps.size() > 0) {
			jsonNode.put("props", nodeProps);
		}
		
		ArrayNode labelArray = mapper.createArrayNode();
		for (Label oneLabel : oneNode.getLabels()) {
			labelArray.add(oneLabel.name());
		}
		jsonNode.put("labels", labelArray);
		
		ArrayNode relationArray = mapper.createArrayNode();
		
		Iterable<Relationship> relationships = oneNode.getRelationships(Direction.OUTGOING);
		for (Relationship oneRel : relationships) {
			ObjectNode relationNode = mapper.createObjectNode();
			
			Node destNode = oneRel.getOtherNode(oneNode);
			relationNode.put("otherId", destNode.getId());
			
			relationNode.put("type", oneRel.getType().name());
			
			ObjectNode relProps = copyPropertiesToJson(oneRel);
			if (relProps.size() > 0) {
				relationNode.put("props", relProps);
			}
			
			relationArray.add(relationNode);
		}
		if (relationArray.size() > 0) {
			jsonNode.put("rels", relationArray);
		}
		
		printWriter.println(jsonNode.toString());
	}

	private ObjectNode copyPropertiesToJson(PropertyContainer propContainer) {
		ObjectNode props = mapper.createObjectNode();
		
		for (String propKey : propContainer.getPropertyKeys()) {
			Object val = propContainer.getProperty(propKey);
			if (val instanceof Double) {
				props.put(propKey, (Double) val);
			} else if (val instanceof Long) {
				props.put(propKey, (Long) val);
			} else if (val instanceof Integer) {
				props.put(propKey, (Integer) val);
			} else if (val instanceof String) {
				props.put(propKey, (String) val);
			}
		}
		
		return props;
	}
}
