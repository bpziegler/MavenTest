package com.qualia.util;


public class InstanceInfo {
    public String instance;
    public String publicIP;
    public String privateIP;


    @Override
    public String toString() {
        return "InstanceInfo [instance=" + instance + ", publicIP=" + publicIP + ", privateIP=" + privateIP + "]";
    }


    public String getTabLine() {
        return instance + "\t" + publicIP + "\t" + privateIP;
    }
}
