package com.qualia.scoring;


import java.util.ArrayList;
import java.util.List;


public class SemantriaLine {

    private final String url;
    private final String id;
    private final List<String> labelNames;


    public SemantriaLine(String url, String id, List<String> labelNames) {
        this.url = url;
        this.id = id;
        this.labelNames = new ArrayList<String>(labelNames);
    }


    public String getUrl() {
        return url;
    }


    public String getId() {
        return id;
    }


    public List<String> getLabelNames() {
        return labelNames;
    }

}
