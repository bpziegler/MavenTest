package com.qualia.util;


import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Splitter;


public class ProgramCallable implements Callable<ProgramResult> {

    private static final Splitter spaceSplitter = Splitter.on(" ");
    private final String cmdLine;


    public ProgramCallable(final String cmdLine) {
        this.cmdLine = cmdLine;
    }


    @Override
    public ProgramResult call() throws Exception {
        List<String> parts = spaceSplitter.splitToList(cmdLine);

        ProgramResult result = new ProgramResult();

        ProcessBuilder pb = new ProcessBuilder(parts);
        Process process = pb.start();
        result.errCode = process.waitFor();
        result.stdOut = IOUtils.toString(process.getInputStream());
        result.stdErr = IOUtils.toString(process.getErrorStream());

        // System.out.println("Finished " + cmdLine);

        return result;
    }
}
