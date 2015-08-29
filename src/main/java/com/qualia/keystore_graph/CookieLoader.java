package com.qualia.keystore_graph;

/*
 * TODO:
 * 
 * - Check if files have already been loaded
 * 
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Splitter;

import net.sf.uadetector.OperatingSystemFamily;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentFamily;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class CookieLoader extends FileLoader {

	private final ObjectMapper mapper = new ObjectMapper();
	private final Splitter dotSplitter = Splitter.on(".");

	private final UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
	
	public CookieLoader(Status status, File inputFile, String saveName) {
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
		
		// TODO:  Check for multiple IP addresses (proxies, etc)
		String ip = tree.get("ip").asText();		
		int ipInt = convertIpStringtoInt(ip);
		
		String userAgent = tree.get("user-agent").asText();
		ReadableUserAgent agent = parser.parse(userAgent);
		UserAgentFamily browserFamily = agent.getFamily();
		OperatingSystemFamily osFamily = agent.getOperatingSystem().getFamily();

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
				GlobalKey key = GlobalKey.createFromPidUid(pid, uid);
				mapping.add(key);
				storage.saveHashLookup(key, pid + "_" + uid);
				storage.saveProperty(key, PropertyLabel.LAST_SEEN, dateInt);
				if (pid.equals("lr")) {
					storage.saveIPMapping(key, ipInt, dateInt);
					storage.saveProperty(key, PropertyLabel.BROWSER, browserFamily.toString());
					storage.saveProperty(key, PropertyLabel.PLATFORM, osFamily.toString());
				}
			}

			storage.saveMapping(mapping);
		}
	}

	private int convertIpStringtoInt(String ip) {
		List<String> parts = dotSplitter.splitToList(ip);
		if (parts.size() != 4) {
			return -1;
		}
		
		try {
			int result = 0;
			for (int i = 0; i < 4; i++) {
				int val = Integer.parseInt(parts.get(i));
				result = result << 8;
				result += val;
			}
			
			return result;
		} catch (NumberFormatException e) {
			return -1;
		}
	}

}