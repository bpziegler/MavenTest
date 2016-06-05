package com.qualia.memgraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.qualia.keystore_graph.BaseFileLoader;
import com.qualia.keystore_graph.Constants;
import com.qualia.keystore_graph.GlobalKey;
import com.qualia.keystore_graph.Status;

import eu.bitwalker.useragentutils.UserAgent;

public class CookieMemGraphLoader extends BaseFileLoader {

    private final ObjectMapper mapper = new ObjectMapper();
    private final MemGraphMappingProcessor processor;
    private BufferedWriter hashLookupStream;

    public CookieMemGraphLoader(Status status, File inputFile, String saveName, MemGraphMappingProcessor processor,
            BufferedWriter hashLookupStream) {
        super(status, inputFile, saveName);
        this.processor = processor;
        this.hashLookupStream = hashLookupStream;
    }

    @Override
    protected void processFile() throws IOException {
        super.processFile();
        try {
            processor.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void processError(IOException e) {
        super.processError(e);
        try {
            processor.flush();
        } catch (InterruptedException e2) {
            throw new RuntimeException(e2);
        }
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

        String userAgent = tree.get("user-agent").asText();
        UserAgent agent = UserAgent.parseUserAgentString(userAgent);
        String os = agent.getOperatingSystem().getGroup().getName();
        String browser = agent.getBrowser().getGroup().getName();

        MappingList mapping = new MappingList();

        // We will skip mappings when the lr cookie is not read
        boolean lr_cookie_read = false;

        for (JsonNode oneMapping : lineMapping) {
            String pid = oneMapping.get("pid").asText();
            if (pid.equals("lr")) {
                JsonNode readNode = oneMapping.get("read");
                if (readNode != null) {
                    lr_cookie_read = readNode.asBoolean();
                } else {
                    // This is for backwards compatibility. Probably safe to
                    // remove now.
                    lr_cookie_read = true;
                }
            }
        }

        if (lr_cookie_read) {
            for (JsonNode oneMapping : lineMapping) {
                String pid = oneMapping.get("pid").asText();
                String uid = oneMapping.get("uid").asText();
                if (Constants.isBadUID(uid)) {
                    continue;
                }
                GlobalKey key = GlobalKey.createFromPidUid(pid, uid);
                mapping.add(key);
                saveHashLookup(key, pid + "_" + uid);
            }

            processor.addMapping(mapping);
        }
    }

    public void saveHashLookup(GlobalKey globalKey, String origId) throws IOException {
        ObjectNode objNode = mapper.createObjectNode();
        objNode.put("origId", origId);
        objNode.put("key", Arrays.toString(globalKey.getHashValue()));
        synchronized (hashLookupStream) {
            hashLookupStream.write(objNode.toString());
            hashLookupStream.newLine();
        }
    }

}
