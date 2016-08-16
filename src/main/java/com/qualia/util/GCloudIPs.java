package com.qualia.util;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;


public class GCloudIPs {

    public static final String GCLOUD_PATH = "/Users/benziegler/work/google-cloud-sdk/bin/gcloud";
    private final ExecutorService executor = Executors.newCachedThreadPool();


    public Future<ProgramResult> runProgram(String cmdLine) {
        ProgramCallable pc = new ProgramCallable(cmdLine);
        return executor.submit(pc);
    }


    private List<String> parseInstanceGroupList(String output) {
        List<String> result = new ArrayList<String>();
        List<String> lines = Splitter.on("\n").splitToList(output);
        for (String line : lines) {
            List<String> parts = Splitter.on(" ").splitToList(line);
            String instanceGroup = parts.get(0);
            if (instanceGroup.equals("NAME") || instanceGroup.length() == 0)
                continue;
            result.add(instanceGroup);
        }
        return result;
    }


    private void run() throws InterruptedException, ExecutionException, IOException {
        Future<ProgramResult> futureResult = runProgram(GCLOUD_PATH + " compute instance-groups list");
        ProgramResult result = futureResult.get();
        List<String> instanceGroups = parseInstanceGroupList(result.stdOut);

        Map<String, Future<ProgramResult>> instanceGroupList = new HashMap<>();
        for (String instanceGroup : instanceGroups) {
            System.out.println("Instance group = " + instanceGroup);
            Future<ProgramResult> futureInstanceList = runProgram(
                    GCLOUD_PATH + " compute instance-groups list-instances " + instanceGroup);
            instanceGroupList.put(instanceGroup, futureInstanceList);
        }

        Map<String, Future<ProgramResult>> instanceList = new HashMap<>();
        for (String instanceGroup : instanceGroupList.keySet()) {
            ProgramResult instanceGroupResult = instanceGroupList.get(instanceGroup).get();
            List<String> instances = parseInstanceGroupList(instanceGroupResult.stdOut); // Happens to be same parsing
                                                                                         // logic!
            System.out.println(instanceGroup + " result = " + instances);
            for (String instance : instances) {
                Future<ProgramResult> futureInstanceInfo = runProgram(
                        GCLOUD_PATH + " compute instances describe " + instance);
                instanceList.put(instance, futureInstanceInfo);
            }
        }

        List<String> output = new ArrayList<String>();
        for (String instanceKey : instanceList.keySet()) {
            ProgramResult instanceResult = instanceList.get(instanceKey).get();
            InstanceInfo instanceInfo = parseInstanceInfo(instanceResult.stdOut);
            instanceInfo.instance = instanceKey;
            System.out.println(instanceInfo);
            output.add(instanceInfo.getTabLine());
        }

        Collections.sort(output);
        FileOutputStream fs = new FileOutputStream("google_instances.txt");
        OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(osw);
        for (String line : output) {
            System.out.println(line);
            bw.write(line);
            bw.newLine();
        }
        bw.close();

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Done");
    }


    private InstanceInfo parseInstanceInfo(String output) {
        InstanceInfo result = new InstanceInfo();

        // System.out.println("output = " + output);

        Pattern publicRegex = Pattern.compile("natIP: (.*?)$", Pattern.MULTILINE);
        Pattern privateRegex = Pattern.compile("networkIP: (.*?)$", Pattern.MULTILINE);

        Matcher publicMatcher = publicRegex.matcher(output);
        if (publicMatcher.find()) {
            result.publicIP = publicMatcher.group(1);
        }

        Matcher privateMatcher = privateRegex.matcher(output);
        if (privateMatcher.find()) {
            result.privateIP = privateMatcher.group(1);
        }

        return result;
    }


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("GCloudIPs");
        GCloudIPs gcloudIPs = new GCloudIPs();
        gcloudIPs.run();
        long elap = System.currentTimeMillis() - startTime;
        System.out.println("elap = " + elap);
    }

}
