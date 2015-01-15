package com.qualia.bluecava;


import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;
import com.localresponse.util.CountMap;


public class BlueCavaStats {

    private Splitter tabSplitter = Splitter.on("\t");
    private final HashFunction hashFunction = Hashing.murmur3_128(); // Simulates MD5 hash (but only 8 bytes)
    private final TLongHashSet hashSet = new TLongHashSet();
    // private final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
    private final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("YYYYMMdd");
    private final LocalDate now = (new DateTime(2014, 12, 22, 0, 0)).toLocalDate();
    private final HashMap<Integer, Integer> daysMap = new HashMap<Integer, Integer>();
    private final ObjectMapper mapper = new ObjectMapper();

    private final CountMap screenPlatformCounts = new CountMap();
    private final CountMap OSCounts = new CountMap();
    private final CountMap OSVersionCounts = new CountMap();
    private final CountMap platformIDTypeCounts = new CountMap();


    private void run(String tapadDir) throws IOException {
        File[] files = (new File(tapadDir)).listFiles();
        List<File> list = Arrays.asList(files);

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        // multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                BlueCavaStats.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = BlueCavaStats.this.getStatus();

                return status;
            }

        });
    }


    private String safeWriteValueAsString(Object obj) {
        String json = null;
        try {
            json = mapper.writeValueAsString(daysMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return json;
    }


    protected String getStatus() {
        try {
            String daysJson = safeWriteValueAsString(daysMap);
            Files.write(daysJson, new File("days_histogram.json"), Charsets.UTF_8);

            String screenReport = screenPlatformCounts.report();
            String osReport = OSCounts.report();
            String osVersionReport = OSVersionCounts.report();
            String platformIDTypeReport = platformIDTypeCounts.report();

            Files.write(screenReport, new File("screen_counts.json"), Charsets.UTF_8);
            Files.write(osReport, new File("os_counts.json"), Charsets.UTF_8);
            Files.write(osVersionReport, new File("os_version_counts.json"), Charsets.UTF_8);
            Files.write(platformIDTypeReport, new File("platform_id_type_counts.json"), Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return String.format("%,10d households   (also writing files each status)", hashSet.size());
    }


    private void report() {
        // TODO Auto-generated method stub

    }


    protected void processLine(String line, long curLine) {
        if (curLine == 0) {
            return;
        }

        List<String> parts = tabSplitter.splitToList(line);

        // 0 HouseholdID, 1 ConsumerID, 2 MetroCode, 3 City, 4 State, 5 Latitude, 6 Longitude, 7 HouseHoldLastSeen, 8
        // ScreenID, 9 OS, 10 OSVersion, 11 ScreenPlatform, 12 PlatformID, 13 PlatformIDType, 14 PlatformIDLastSeen

        String householdId = parts.get(0);
        long hash = hashFunction.hashString(householdId, Charsets.UTF_8).asLong();
        hashSet.add(hash);

        String lastSeen = parts.get(14);
        if (!lastSeen.equals("NULL")) {
            LocalDate lastSeenDT = dateFmt.parseLocalDate(lastSeen);

            Integer numDays = Integer.valueOf(Days.daysBetween(lastSeenDT, now).getDays());
            Integer cur = daysMap.get(numDays);
            if (cur == null)
                cur = 0;
            cur = cur + 1;
            daysMap.put(numDays, cur);
        }

        String screenPlatform = parts.get(11);
        String OS = parts.get(9);
        String OSVersion = parts.get(9) + " " + parts.get(10);
        String platformIDType = parts.get(13);

        screenPlatformCounts.updateCount(screenPlatform);
        OSCounts.updateCount(OS);
        OSVersionCounts.updateCount(OSVersion);
        platformIDTypeCounts.updateCount(platformIDType);
    }


    public static void main(String[] args) throws IOException {
        String dir = null;

        if (args.length > 0) {
            dir = args[0];
            System.out.println("tapadDir = " + dir);
        }

        BlueCavaStats program = new BlueCavaStats();
        program.run(dir);
    }

}
