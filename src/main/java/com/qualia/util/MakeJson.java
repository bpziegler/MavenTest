package com.qualia.util;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.google.common.base.Charsets;


/*
 * Loads in a file, and creates a new file that has the line embedded in a JSON hash.
 * Used to read a rocks db dump, and convert from pid_uid per line to a JSON hash of { pid_uid: "adnxs_123" }
 */
public class MakeJson implements ILineProcessor {
    private final ObjectMapper mapper = new ObjectMapper();
    private BufferedWriter bw;


    private void run(String inputPath, String outputPath) throws IOException {
        FileOutputStream fs = new FileOutputStream(outputPath);
        GZIPOutputStream gz = new GZIPOutputStream(fs);
        OutputStreamWriter osw = new OutputStreamWriter(gz, Charsets.UTF_8);
        bw = new BufferedWriter(osw);

        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.processOneFile(new File(inputPath), this);

        bw.close();
    }


    @Override
    public void processLine(String line, long curLine) {
        ObjectNode obj = mapper.getNodeFactory().objectNode();
        obj.put("pid_uid", line);
        try {
            bw.write(obj.toString());
            bw.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public String getStatus() {
        return null;
    }


    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = inputPath + "_json.gz";

        MakeJson program = new MakeJson();
        program.run(inputPath, outputPath);
    }

}
