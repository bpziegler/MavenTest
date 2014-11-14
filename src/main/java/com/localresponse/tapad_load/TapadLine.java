package com.localresponse.tapad_load;


import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Splitter;


public class TapadLine {

    private static final Splitter tabSplitter = Splitter.on("\t");
    private static final Splitter equalSplitter = Splitter.on("=");

    public final List<Device> devices = new ArrayList<Device>();


    public TapadLine(String line) {
        List<String> deviceStrs = tabSplitter.splitToList(line);

        for (String oneDeviceStr : deviceStrs) {
            List<String> parts = equalSplitter.splitToList(oneDeviceStr);
            devices.add(new Device(parts.get(0), parts.get(1), parts.get(2)));
        }
    }
}
