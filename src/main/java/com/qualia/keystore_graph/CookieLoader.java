package com.qualia.keystore_graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class CookieLoader extends FileLoader {

	private final GraphStorage storage;
	private final ObjectMapper mapper = new ObjectMapper();

	public CookieLoader(Status status, File inputFile) {
		super(status, inputFile);
		storage = new GraphStorage(false);
	}

	@Override
	public void processFile() throws IOException {
		super.processFile();
		storage.close();
	}

	@Override
	public void processLine(String line, long curLine) throws Exception {
        JsonNode tree = mapper.readTree(line);

        ArrayNode lineMapping = (ArrayNode) tree.get("mapping");
        if (lineMapping == null)
            return;
        // time = "2015-03-24T00:09:44.935Z"
        String time = tree.get("time").asText();
        String dateOnlyStr = time.substring(0, 10).replace("-", "");
        int dateInt = Integer.valueOf(dateOnlyStr);
        
        List<GlobalKey> mapping = new ArrayList<GlobalKey>();
        
        for (JsonNode oneMapping : lineMapping) {
            String pid = oneMapping.get("pid").asText();
            String uid = oneMapping.get("uid").asText();
            GlobalKey key = GlobalKey.createFromPidUid(pid, uid);
            mapping.add(key);
            storage.saveHashLookup(key, pid + "_" + uid);
            storage.saveProperty(key, PropertyLabel.LAST_SEEN, dateInt);
            // TODO:  Platform, Type, Browser
            // TODO:  Check the "read" flag
            // TODO:  Save IP Mappings
            // TODO:  Mark files as "done"
        }
        
        storage.saveMapping(mapping);
	}

}
