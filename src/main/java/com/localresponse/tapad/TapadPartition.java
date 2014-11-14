package com.localresponse.tapad;


import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class TapadPartition {

	private final GraphPartioner graphParitioner = new GraphPartioner();
	private final Splitter tabSplitter = Splitter.on(Pattern.compile("[\t ]"));
	private final HashFunction hashFunction = Hashing.murmur3_128();
	private final ObjectMapper mapper = new ObjectMapper();

	private int lastNumParition;
	private long lastPartitionCalcTime;
	private int lastMaxPartition;


	private void run(String tapadFile) throws IOException {
		List<File> list = new ArrayList<File>();
		list.add(new File(tapadFile));
		MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
		multiLineProcessor.processFiles(list, new ILineProcessor() {
			public void processLine(String line, long curLine) {
				TapadPartition.this.processLine(line, curLine);
			}


			public String getStatus() {
				if (System.currentTimeMillis() - lastPartitionCalcTime >= 120 * 1000) {
					calcSlowStats();
				}
				
				String status = TapadPartition.this.getStatus();

				return status;
			}

		});
		
		calcSlowStats();
		System.out.println(getStatus());
	}


	private String getStatus() {
		String status = String.format("Map Size = %,12d   Partitions = %,8d   Max Part = %,8d", graphParitioner
				.getLongObjectMap().size(), lastNumParition, lastMaxPartition);
		return status;
	}


	private void calcSlowStats() {
		System.out.println("Calc partitions");
		lastPartitionCalcTime = System.currentTimeMillis();
		lastNumParition = graphParitioner.getNumParititions();
		lastMaxPartition = graphParitioner.getMaxPartitionSize();
		dumpMaxPartition(graphParitioner.getMaxPartition());
	}


	protected void dumpMaxPartition(TLongHashSet maxPartition) {
		long[] ary = maxPartition.toArray();
		ArrayList<Long> nodeArray = new ArrayList<Long>();
		for (long oneVal : ary) {
			nodeArray.add(oneVal);
		}

		try {
			mapper.writeValue(new File("max_partition_elements.json"), nodeArray);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	protected void processLine(String line, long curLine) {
		List<String> devices = tabSplitter.splitToList(line);

		TLongArrayList nodes = new TLongArrayList();
		for (String oneDevice : devices) {
			HashCode hash = hashFunction.hashString(oneDevice, Charsets.UTF_8);
			nodes.add(hash.asLong());
		}

		graphParitioner.addRelatedNodes(nodes);
	}


	public static void main(String[] args) throws Exception {
		String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

		if (args.length > 0) {
			tapadFile = args[0];
			System.out.println("tapadFile = " + tapadFile);
		}

		TapadPartition tapadPartition = new TapadPartition();

		tapadPartition.run(tapadFile);
	}

}
