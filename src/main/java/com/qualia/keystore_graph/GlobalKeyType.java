package com.qualia.keystore_graph;


public enum GlobalKeyType {
    ENTITY, DEVICE, COOKIE, IP, APPNEXUS("adnxs"), LOTAME("ltm");

    private final String pid;


    GlobalKeyType() {
        this.pid = null;
    }


    GlobalKeyType(String pid) {
        this.pid = pid;
    }


    public String getPid() {
        return pid;
    }


    public static GlobalKeyType fromPid(String pid) {
        GlobalKeyType result = COOKIE;

        for (GlobalKeyType one : GlobalKeyType.values()) {
            if (pid.equals(one.getPid())) {
                result = one;
                break;
            }
        }

        return result;
    }
}
