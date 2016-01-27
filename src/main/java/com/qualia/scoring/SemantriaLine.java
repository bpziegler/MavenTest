package com.qualia.scoring;

import java.util.ArrayList;
import java.util.List;

public class SemantriaLine {

    private final String url;
    private final List<String> labelNames;

    public SemantriaLine(String url, List<String> labelNames) {
        this.url = url;
        this.labelNames = new ArrayList<String>(labelNames);
    }

    public String getUrl() {
        return url;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

}
