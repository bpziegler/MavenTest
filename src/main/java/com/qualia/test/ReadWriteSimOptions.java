package com.qualia.test;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Splitter;


public class ReadWriteSimOptions {
    public static final int PAGE_SIZE = 4096;

    public int fileSizeGB;
    public int simTimeSec;
    public int readThreads;
    public int writeThreads;
    public List<Integer> readSizes;
    public List<Integer> writeSizes;
    public Boolean useCache;

    public long fileLen;
    public final AtomicInteger numReads = new AtomicInteger();
    public final AtomicInteger numWrites = new AtomicInteger();


    private static List<Integer> parseSizesStr(String sizesStr) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        Splitter commaSplitter = Splitter.on(",");
        List<String> parts = commaSplitter.splitToList(sizesStr);
        for (String onePart : parts) {
            result.add(Integer.valueOf(onePart));
        }
        return result;
    }


    public static ReadWriteSimOptions createFromCmdLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("fs", "file-size", true,
                "Size of test file (in GB) used for reading and writing (default 16)");
        options.addOption("st", "sim-time", true,
                "Simulation time (in seconds) to read and write to file (default 60)");
        options.addOption("rt", "read-threads", true, "Number of read threads (default 4)");
        options.addOption("wt", "write-threads", true, "Number of write threads (default 4)");
        options.addOption("rs", "read-size", true,
                "Read size in bytes, comma seperated list, picks read size with uniform random number (default 4096)");
        options.addOption("ws", "write-size", true,
                "Write size in bytes, comma seperated list, picks read size with uniform random number (default 4096");
        options.addOption("c", "use-cache", true,
                "Use cache.  If false all writes are flushed immediately to disk (default true)");
        options.addOption("h", "help", false, "Print usage help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("RandomReadWriteTest", options);
            return null;
        }

        ReadWriteSimOptions simOptions = new ReadWriteSimOptions();

        simOptions.fileSizeGB = Integer.valueOf(cmd.getOptionValue("file-size", "16"));
        simOptions.simTimeSec = Integer.valueOf(cmd.getOptionValue("sim-time", "60"));
        simOptions.readThreads = Integer.valueOf(cmd.getOptionValue("read-threads", "4"));
        simOptions.writeThreads = Integer.valueOf(cmd.getOptionValue("write-threads", "4"));
        String readSizesStr = cmd.getOptionValue("read-size", "4096");
        String writeSizesStr = cmd.getOptionValue("write-size", "4096");
        simOptions.readSizes = parseSizesStr(readSizesStr);
        simOptions.writeSizes = parseSizesStr(writeSizesStr);
        simOptions.useCache = Boolean.valueOf(cmd.getOptionValue("use-cache", "true"));

        return simOptions;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReadWriteSimOptions [fileSizeGB=");
        builder.append(fileSizeGB);
        builder.append(", simTimeSec=");
        builder.append(simTimeSec);
        builder.append(", readThreads=");
        builder.append(readThreads);
        builder.append(", writeThreads=");
        builder.append(writeThreads);
        builder.append(", readSizes=");
        builder.append(readSizes);
        builder.append(", writeSizes=");
        builder.append(writeSizes);
        builder.append(", useCache=");
        builder.append(useCache);
        builder.append("]");
        return builder.toString();
    }
}
