package com.localresponse.tapad;


import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class TapadAnalyze {

	private static final int BATCH_SIZE = 1000;
	private final Splitter tabSplitter = Splitter.on(Pattern.compile("[\t ]"));
	private final Splitter equalSplitter = Splitter.on("=");
	private final Map<String, String> lookupCodes = new TreeMap<String, String>();
	private final ObjectMapper mapper = new ObjectMapper();
	private DB db;
	private WriteBatch batch;
	private int numBatch;
	private BufferedWriter bw;
	
	public TapadAnalyze() {
	}


	private void createGraphFile(String tapadFile) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		db = factory.open(new File("leveldb-keystore"), options);
		
		FileOutputStream fs = new FileOutputStream("text_graph.txt");
		bw = new BufferedWriter(new OutputStreamWriter(fs), 128 * 1024);

		try {
			runLineProcessor(tapadFile);
			
			if (batch != null) {
				db.write(batch);
			}
		} finally {
			bw.close();
			db.close();
		}
	}


	private void runLineProcessor(String tapadFile) throws IOException {
		List<File> list = new ArrayList<File>();
		list.add(new File(tapadFile));
		MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
		multiLineProcessor.processFiles(list, new ILineProcessor() {
			private int lastCodeSize;

			public void processLine(String line, long curLine) {
				TapadAnalyze.this.processLine(line, curLine);
			}

			public String getStatus() {
				if (lookupCodes.size() != lastCodeSize) {
					lastCodeSize = lookupCodes.size();
					try {
						String codesJson = mapper.writeValueAsString(lookupCodes);
						Files.write(codesJson, new File("codes.json"), Charsets.UTF_8);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				
				return String.format("#Codes %3d", lookupCodes.size());
			}
		});
	}


	protected void processLine(String line, long curLine) {
		List<String> devices = tabSplitter.splitToList(line);

		// Create a shared "node" for all devices "Line-curLine", link each device to that node (bi-directional)
		String sharedNode = "L" + Long.toHexString(curLine);
		
		processLine_text(devices, sharedNode);
	}

	
	protected void processLine_text(List<String> devices, String sharedNode) {
		for (String oneDevice : devices) {
			String compressedDevice = getCompressedDevice(oneDevice);
			
			try {
				bw.write(compressedDevice + "\t" + sharedNode + "\n");
				bw.write(sharedNode + "\t" + compressedDevice + "\n");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	
	protected void processLine_LevelDB(List<String> devices, String sharedNode) {
		if (batch == null) {
			batch = db.createWriteBatch();
		}

		for (String oneDevice : devices) {
			String compressedDevice = getCompressedDevice(oneDevice);
			
			// Terminating tabs are required, so that we don't mistake key 123 and 1234 as being the same (i.e., 123\t won't match 1234\t)
			batch.put(bytes(compressedDevice + "\t" + sharedNode + "\t"), bytes(""));
			batch.put(bytes(sharedNode + "\t" + compressedDevice + "\t"), bytes(""));
		}
		
		numBatch++;
		if (numBatch >= BATCH_SIZE) {
			db.write(batch);
			batch = null;
		}
	}


	private String getCompressedDevice(String oneDevice) {
		if (!oneDevice.contains("=")) {
			oneDevice = "0=" + oneDevice + "=0";
		}
		
		List<String> deviceParts = equalSplitter.splitToList(oneDevice);

		String deviceType = deviceParts.get(0);
		String uid = deviceParts.get(1);
		String platform = deviceParts.get(2);

		String deviceTypeCode = findCreateCode(deviceType);
		String platformCode = findCreateCode(platform);
		String compressedDevice = deviceTypeCode + "/" + uid + "/" + platformCode;

		return compressedDevice;
	}


	private String findCreateCode(String value) {
		String code = lookupCodes.get(value);
		if (code == null) {
			code = Integer.toString(lookupCodes.size());
			lookupCodes.put(value, code);
		}

		return code;
	}


	public static void main(String[] args) throws Exception {
		String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

		if (args.length > 0) {
			tapadFile = args[0];
			System.out.println("tapadFile = " + tapadFile);
		}

		TapadAnalyze tapadAnalyze = new TapadAnalyze();
		tapadAnalyze.createGraphFile(tapadFile);
	}


}
