package com.qualia.hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.google.common.base.Splitter;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;

public class AddThisMappingLoader {

	private TSocket transport;
	private Client client;
	private ObjectMapper mapper = new ObjectMapper();
	private Splitter tabSplitter = Splitter.on("\t");
	private Splitter commaSplitter = Splitter.on(",");
	private Splitter equalSplitter = Splitter.on("=");
	private HashMap<String, String> pidCodes = new HashMap<String, String>();
	private List<Mapping> readyMappings = new ArrayList<Mapping>();
	private List<Mapping> waitingMappings = new ArrayList<Mapping>();
	private Set<String> locks = new HashSet<String>();
	
	private static class Mapping {
		public String deviceKey1;
		public String deviceKey2;
		public Mapping(String deviceKey1, String deviceKey2) {
			super();
			this.deviceKey1 = deviceKey1;
			this.deviceKey2 = deviceKey2;
		}
	}

	public AddThisMappingLoader() {
		transport = new TSocket("localhost", 9090);
		TBinaryProtocol protocol = new TBinaryProtocol(transport, true, true);
		client = new Hbase.Client(protocol);
		pidCodes.put("6", "adnxs");
		pidCodes.put("9", "fat");
	}

	private void load(String addThisMappingFile) throws Exception {
		transport.open();

		List<File> list = new ArrayList<File>();
		list.add(new File(addThisMappingFile));

		MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
		multiLineProcessor.processFiles(list, new ILineProcessor() {
			public void processLine(String line, long curLine) {
				try {
					AddThisMappingLoader.this.processLine(line, curLine);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			public String getStatus() {
				return null;
			}
		});

		transport.close();
	}

	protected void processLine(String line, long curLine) throws Exception {
		List<String> parts = tabSplitter.splitToList(line);
		String timestampStr = parts.get(0);
		DateTime dt = new DateTime(Long.valueOf(timestampStr));
		String dtStr = dt.toString();
		// System.out.println(dt);
		String lr_uid = parts.get(1);
		String pidUidPairsList = parts.get(2);
		List<String> pidUidPairs = commaSplitter.splitToList(pidUidPairsList);
		String lrDeviceKey = "pu_" + "lr_" + lr_uid;
		setKeyValBatch("props", lrDeviceKey, "last_seen", dtStr);
		for (String pidUidPair : pidUidPairs) {
			List<String> pidUidParts = equalSplitter.splitToList(pidUidPair);
			String pidCode = pidUidParts.get(0);
			String pid = pidCodes.get(pidCode);
			String uid = pidUidParts.get(1);
			if (uid == "0" || uid == "-1") {
				continue;
			}
			String mappedDeviceKey = "pu_" + pid + "_" + uid;
			setKeyValBatch("props", mappedDeviceKey, "last_seen", dtStr);
			createMapping(lrDeviceKey, mappedDeviceKey);
		}
		
		if (readyMappings.size() >= 100 || waitingMappings.size() >= 400) {
			// System.out.println("Batch complete " + readyMappings.size() + " " + waitingMappings.size());
			// Do nothing
			readyMappings.clear();
			locks.clear();
			
			List<Mapping> list = new ArrayList<Mapping>();
			list.addAll(waitingMappings);
			waitingMappings.clear();
			for (Mapping oneMapping : list) {
				assignMapping(oneMapping);
			}
		}
	}

	private void createMapping(String lrDeviceKey, String mappedDeviceKey) {
		Mapping mapping = new Mapping(lrDeviceKey, mappedDeviceKey);
		assignMapping(mapping);
	}
	
	private void assignMapping(Mapping mapping) {
		if (getLocks(mapping)) {
			readyMappings.add(mapping);
		} else {
			waitingMappings.add(mapping);
		}
	}

	private boolean getLocks(Mapping mapping) {
		boolean free1 = !locks.contains(mapping.deviceKey1);
		boolean free2 = !locks.contains(mapping.deviceKey2);
		if (free1 && free2) {
			locks.add(mapping.deviceKey1);
			locks.add(mapping.deviceKey2);
			return true;
		} else {
			return false;
		}
	}

	private void setKeyValBatch(String tableName, String key, String colName,
			String colValue) {
	}

	public static void main(String[] args) throws Exception {
		AddThisMappingLoader loader = new AddThisMappingLoader();
		String path = (args.length > 0) ? args[0] : null;
		if (path == null) {
			path = "test_data/batch-uids-localresponse-150627_20150628065001";
		}
		loader.load(path);
	}
}
