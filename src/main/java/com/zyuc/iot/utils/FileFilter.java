package com.zyuc.iot.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhoucw on 下午2:50.
 */
public class FileFilter  implements FilenameFilter {
    private String filterReg;

    public FileFilter(String filterReg) {
        this.filterReg = filterReg;
    }

    @Override
    public boolean accept(File dir, String name) {

        return name.matches(filterReg);
    }
    public static void main(String[] args) {
        File dir = new File("/hadoop/bd/b1");
        File[] files = dir.listFiles(new FileFilter("^p.*[0-9]$"));
        for(File file:files){
            System.out.println(file);
        }


    }
}
