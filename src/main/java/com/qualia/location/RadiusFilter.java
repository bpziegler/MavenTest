package com.qualia.location;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;


public class RadiusFilter {

    private final Splitter splitter = Splitter.on(",");
    private BufferedWriter bw;
    private double meters;
    private SpatialContext geo;
    private Point filterPoint;
    private int numMatch;
    protected int lineErrors;


    public void filterFile(String inputPath, String outputPath, double lat, double lon, double meters)
            throws IOException {
        geo = SpatialContext.GEO;

        FileOutputStream fs = new FileOutputStream(outputPath);
        OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
        bw = new BufferedWriter(osw);
        this.meters = meters;
        this.filterPoint = geo.makePoint(lon, lat);


        MultiFileLineProcessor processor = new MultiFileLineProcessor();
        processor.processDir(inputPath, new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    RadiusFilter.this.processLine(line, curLine);
                } catch (IOException e) {
                    lineErrors++;
                }
            }


            @Override
            public String getStatus() {
                return String.format("%,d matches   %,d lineErrors", numMatch, lineErrors);
            }
        });

        bw.close();
    }


    protected void processLine(String line, long curLine) throws IOException {
        List<String> parts = splitter.splitToList(line);
        double lineLat = Double.valueOf(parts.get(3));
        double lineLong = Double.valueOf(parts.get(4));
        Point linePoint = geo.makePoint(lineLong, lineLat);
        double degrees = geo.calcDistance(linePoint, filterPoint);
        double disKM = degrees / (180 / Math.PI) * 6378.1; // kilometers of the radius of the earth = 6378.1
        double disMeters = disKM * 1000;
        if (disMeters <= meters) {
            numMatch++;
            bw.write(String.format("%1.2f,%s", disMeters, line));
            bw.newLine();
        }
    }


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("i", "input", true, "input dir (fiels in Qualia location format)");
        options.addOption("o", "output", true, "output file (Qualia location with distance prepended)");
        options.addOption("t", "lat", true, "latitude (x), default 40.745686 for Qualia");
        options.addOption("g", "long", true, "longitude (y), default -73.989565 for Qualia");
        options.addOption("d", "distance", true, "distance in meters to include location points, default 1000");
        options.addOption("h", "help", false, "Print usage help");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("RadiusFilter", options);
            return;
        }

        Double lat = Double.valueOf(cmd.getOptionValue("lat", "40.745686"));
        Double lon = Double.valueOf(cmd.getOptionValue("long", "-73.989565"));
        Double dis = Double.valueOf(cmd.getOptionValue("distance", "1000"));
        String inputPath = cmd.getOptionValue("input");
        String outputPath = cmd.getOptionValue("output");

        RadiusFilter program = new RadiusFilter();
        program.filterFile(inputPath, outputPath, lat, lon, dis);
    }

}
