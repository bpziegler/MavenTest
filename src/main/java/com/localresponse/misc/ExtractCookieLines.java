package com.localresponse.misc;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class ExtractCookieLines {

    private BufferedWriter bw;
    private Set<String> uids = new HashSet<String>();
    private ObjectMapper mapper = new ObjectMapper();
    private int numMatch;


    private void run(String cookieDir) throws IOException {
        FileOutputStream fs = new FileOutputStream("extracted_cookie_lines.txt");
        bw = new BufferedWriter(new OutputStreamWriter(fs), 128 * 1024);

        List<File> list = new ArrayList<File>();

        File file = new File(cookieDir);
        if (file.isDirectory()) {
            File[] allFiles = (new File(cookieDir)).listFiles();
            for (File oneFile : allFiles) {
                if (oneFile.getAbsolutePath().endsWith(".log")) {
                    list.add(oneFile);
                }
            }
        } else {
            list.add(file);
            cookieDir = file.getParent();
        }

        loadMatchFile(cookieDir);

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                ExtractCookieLines.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = String.format("%,8d", numMatch);

                return status;
            }

        });

        bw.close();
    }


    private void loadMatchFile(String cookieDir) throws IOException {
        File matchFile = new File(cookieDir, "match.txt");
        List<String> lines = Files.readAllLines(matchFile.toPath(), Charsets.UTF_8);

        Splitter splitter = Splitter.on(",");

        for (String oneLine : lines) {
            List<String> parts = splitter.splitToList(oneLine);
            String uid = parts.get(1);
            uids.add(uid);
        }
    }


    protected void processLine(String line, long curLine) {
        try {
            boolean match = false;

            // JsonNode json = mapper.readTree(line);
            // ArrayNode mapping = (ArrayNode) json.get("mapping");
            // if (mapping != null) {
            // for (JsonNode oneMapping : mapping) {
            // String uid = oneMapping.get("uid").asText();
            // match = match || uids.contains(uid);
            // }
            // }

            for (String oneUid : uids) {
                match = match || (line.contains(oneUid));
            }

            if (match) {
                numMatch++;
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws IOException {
        // String cookieDir = "/Users/benziegler/test_data/cookies";
        String cookieDir = "/Users/benziegler/test_data/cookies/batch-uids-localresponse-141208_20141209065001";

        if (args.length > 0) {
            cookieDir = args[0];
            System.out.println("cookieDir = " + cookieDir);
        }

        ExtractCookieLines program = new ExtractCookieLines();

        program.run(cookieDir);
    }

}
