package com.localresponse.tapad;


import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class TapadPartition {

    private final GraphPartioner graphParitioner = new GraphPartioner();
    private final Splitter tabSplitter = Splitter.on(Pattern.compile("[\t ]"));
    private final HashFunction hashFunction = Hashing.murmur3_128();
    private final ObjectMapper mapper = new ObjectMapper();
    private final File checkFile = new File("dumpstats");


    private void run(String tapadDir) throws IOException {
        List<File> list = new ArrayList<File>();
        File tapadDirFile = new File(tapadDir);
        File[] files = tapadDirFile.listFiles();
        for (File oneFile : files) {
            list.add(oneFile);
        }

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                TapadPartition.this.processLine(line, curLine);
            }


            public String getStatus() {
                return TapadPartition.this.getStatus();
            }

        });

        dumpPartitionStats();

        System.out.println(getStatus());
    }


    private void dumpPartitionStats() throws FileNotFoundException, IOException {
        System.out.println("Dumping Partition Stats");
        FileOutputStream fs = new FileOutputStream("partition_stats.txt");
        graphParitioner.dumpPartitionStats(fs);
    }


    private String getStatus() {
        if (checkFile.exists()) {
            try {
                dumpPartitionStats();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            checkFile.delete();
        }

        String status = String.format("Map Size = %,12d   Max Part = %,8d", graphParitioner.getLongObjectMap().size(),
                graphParitioner.getMaxPartitionSize());
        return status;
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

        if (devices.size() > 15) {
            return;
        }

        TLongArrayList nodes = new TLongArrayList();
        for (String oneDevice : devices) {
            HashCode hash = hashFunction.hashString(oneDevice, Charsets.UTF_8);
            nodes.add(hash.asLong());
        }

        graphParitioner.addRelatedNodes(nodes);
    }


    public static void main(String[] args) throws Exception {
        String tapadDir = "/Users/benziegler/work/tapad/multfiles";

        if (args.length > 0) {
            tapadDir = args[0];
            System.out.println("tapadDir = " + tapadDir);
        }

        TapadPartition tapadPartition = new TapadPartition();

        tapadPartition.run(tapadDir);
    }

}
