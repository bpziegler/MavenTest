package com.qualia.memgraph;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.qualia.keystore_graph.BaseFileLoader;
import com.qualia.keystore_graph.Constants;
import com.qualia.keystore_graph.GlobalKey;
import com.qualia.keystore_graph.Status;

import eu.bitwalker.useragentutils.UserAgent;

public class CookieMemGraphLoader extends BaseFileLoader {

    private final ObjectMapper mapper = new ObjectMapper();

    public CookieMemGraphLoader(Status status, File inputFile, String saveName) {
        super(status, inputFile, saveName);
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

        List<GlobalKey> mapping = new ArrayList<GlobalKey>();

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
                // TODO:  Save properties to key value store
//                storage.saveHashLookup(key, pid + "_" + uid);
//                storage.saveProperty(key, PropertyLabel.LAST_SEEN, dateInt);
//                if (pid.equals("lr")) {
//                    storage.saveIPMapping(key, ipInt, dateInt);
//                    storage.saveProperty(key, PropertyLabel.BROWSER, browser);
//                    storage.saveProperty(key, PropertyLabel.PLATFORM, os);
//                }
            }

            // storage.saveMapping(mapping);
        }
    }

}
