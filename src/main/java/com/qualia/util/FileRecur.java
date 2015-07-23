package com.qualia.util;


import java.io.File;
import java.util.List;


public class FileRecur {

    public static void getFilesInDirRecursively(String path, List<File> files) {

        File root = new File(path);
        File[] list = root.listFiles();

        if (list == null)
            return;

        for (File f : list) {
            if (f.isDirectory()) {
                getFilesInDirRecursively(f.getAbsolutePath(), files);
            } else {
                files.add(f);
            }
        }
    }

}
