package com.qualia.scoring;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.google.common.io.CountingInputStream;
import com.qualia.keystore_graph.Status;


public class ParseSemantriaThread implements Runnable {

    private static final int STATUS_LINES_SIZE = 100;

    private static final int SEMANTRIA_LINE_BATCH_SIZE = 100;

    private final String filePath;
    private final BlockingQueue<Object> queue;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Status status;

    private SemantriaLineBatch semantriaLineBatch = new SemantriaLineBatch();
    private int numLines;
    private long lastCount;
    private long numBytes;


    public ParseSemantriaThread(String filePath, BlockingQueue<Object> queue, Status status) {
        this.filePath = filePath;
        this.queue = queue;
        this.status = status;
    }


    public void safeRun() throws IOException, InterruptedException {
        FileInputStream fs = new FileInputStream(filePath);
        CountingInputStream cs = new CountingInputStream(fs);
        InputStream nextStream = cs;
        nextStream = new GZIPInputStream(nextStream);
        InputStreamReader isr = new InputStreamReader(nextStream);
        BufferedReader br = new BufferedReader(isr);

        String line;

        while ((line = br.readLine()) != null) {
            processLine(line);

            long curBytes = cs.getCount() - lastCount;
            lastCount = cs.getCount();
            numBytes += curBytes;
            numLines++;
            if (numLines >= STATUS_LINES_SIZE) {
                status.numLines.addAndGet(numLines);
                status.numBytes.addAndGet(numBytes);
                numLines = 0;
                numBytes = 0;
            }
        }

        br.close();
    }


    private void processLine(String line) throws IOException, InterruptedException {
        JsonNode jsonTree = mapper.readTree(line);
        String id = jsonTree.get("id").asText();
        String url = jsonTree.get("url").asText();
        String jobStatus = jsonTree.get("status").asText();
        ArrayNode topics = (ArrayNode) jsonTree.get("topics");
        if (id == null)
            return;
        if (url == null)
            return;
        if (status == null)
            return;
        if (!jobStatus.equals("PROCESSED"))
            return;
        if (topics == null)
            return;
        if (topics.size() == 0)
            return;

        List<String> labelNames = new ArrayList<String>();
        for (JsonNode topicNode : topics) {
            labelNames.add(topicNode.get("title").asText());
        }

        SemantriaLine semantriaLine = new SemantriaLine(url, id, labelNames);
        semantriaLineBatch.add(semantriaLine);

        if (semantriaLineBatch.size() >= SEMANTRIA_LINE_BATCH_SIZE) {
            queue.put(semantriaLineBatch);
            semantriaLineBatch = new SemantriaLineBatch();
        }
    }


    @Override
    public void run() {
        try {
            safeRun();

            if (semantriaLineBatch.size() > 0) {
                queue.put(semantriaLineBatch);
                semantriaLineBatch = new SemantriaLineBatch();
            }

            status.numFiles.incrementAndGet();
            status.numLines.addAndGet(numLines);
            status.numBytes.addAndGet(numBytes);
        } catch (IOException e) {
            status.numErrors.incrementAndGet();
        } catch (InterruptedException e) {
            status.numErrors.incrementAndGet();
        }
    }

}
