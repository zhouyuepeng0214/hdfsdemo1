package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        //1.客户端获取
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop110:9000"),
                new Configuration(), "atguigu");
        boolean mkdirs = fileSystem.mkdirs(new Path("/tttt"));
        if (mkdirs) {
            System.out.println("ok");
        } else {
            System.out.println("nok");
        }
        fileSystem.close();
    }
    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs  = FileSystem.get(URI.create("hdfs://hadoop110:9000"),
                new Configuration(), "atguigu");
    }

    @Test
    public  void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getModificationTime());
            if (fileStatus.isFile()) {
                System.out.println(fileStatus.getBlockSize());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getReplication());
                long blockSize = fileStatus.getBlockSize();
            } else {
                System.out.println("zheshiwenjian");
            }
        }
    }

    @After
    public void after() throws IOException {
        fs.close();
    }
}
