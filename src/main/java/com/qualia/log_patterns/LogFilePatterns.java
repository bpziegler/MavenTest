package com.qualia.log_patterns;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Charsets;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class LogFilePatterns {

    private static final int MIN_LINES_TO_SAVE_NODE = 10000;
    private static final int MAX_LINES_TO_READ = 1000000;


    public void run(String logPath) throws IOException {
        List<String> lines = loadLines(logPath);
        Node node = new Node();
        node.lines.addAll(lines);
        node.checkSplitNode();

        System.out.println("Sorting nodes");
        List<Node> leafNodes = getLeafNodesSorted(node);

        System.out.println("Saving nodes");
        writeAllNodes(leafNodes);

        System.out.println("Done");
    }


    private List<Node> getLeafNodesSorted(Node node) {
        final List<Node> leafNodes = new ArrayList<Node>();

        node.visitNodes(new IVisitCallback() {
            @Override
            public void visitNode(Node node) {
                if (node.isLeafNode()) {
                    leafNodes.add(node);
                }
            }
        });

        Collections.sort(leafNodes, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return Integer.compare(o2.lines.size(), o1.lines.size());
            }
        });

        return leafNodes;
    }


    public static void writeAllNodes(List<Node> leafNodes) throws IOException {
        final BufferedWriter bw = createBufferedWriter(new File("log_nodes.txt"));

        for (Node node : leafNodes) {
            writeNode(bw, node);

            if (node.isLeafNode() && node.lines.size() > MIN_LINES_TO_SAVE_NODE) {
                saveNodeToFile(node);
            }
        }

        bw.close();
    }


    private static void saveNodeToFile(Node node) throws IOException {
        File dir = new File("saved_nodes");
        dir.mkdirs();

        String name = node.conditionString();
        name = name.replaceAll(" ", "_");
        name = name.replaceAll("!", "-");
        name = name + ".txt";
        System.out.println("Saving " + name);
        File node_save_path = new File(dir, name);
        BufferedWriter bw = createBufferedWriter(node_save_path);

        for (String line : node.lines) {
            bw.write(line);
            bw.newLine();
        }

        bw.close();
    }


    private static BufferedWriter createBufferedWriter(File path) throws FileNotFoundException {
        FileOutputStream fs = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
        final BufferedWriter bw = new BufferedWriter(osw);
        return bw;
    }


    public static void writeNode(BufferedWriter bw, Node node) throws IOException {
        bw.write(node.conditionString());
        bw.newLine();
        for (int i = 0; i < node.lines.size(); i++) {
            if (i < 10) {
                bw.write(String.format("%d of %d   %s", i, node.lines.size(), node.lines.get(i)));
                bw.newLine();
            }
        }
        bw.newLine();
    }


    private List<String> loadLines(String logPath) throws IOException {
        final List<String> lines = new ArrayList<String>();

        MultiFileLineProcessor mp = new MultiFileLineProcessor();
        List<File> logFiles = new ArrayList<File>();
        logFiles.add(new File(logPath));
        mp.processFiles(logFiles, new ILineProcessor() {
            int lineNum = 0;


            @Override
            public void processLine(String line, long curLine) {
                lineNum++;
                if (lineNum <= MAX_LINES_TO_READ) {
                    lines.add(line);
                }
            }


            @Override
            public String getStatus() {
                return null;
            }
        });

        return lines;
    }


    public static void main(String[] args) throws IOException {
        String logPath = "/Users/benziegler/test_data/workflow_logs/oIqWorkflow.2016-08-21_17-22-47.log";
        logPath = "/Users/benziegler/messages.log";

        LogFilePatterns prog = new LogFilePatterns();
        prog.run(logPath);
    }

}
