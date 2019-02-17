package com.hank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MyHdfsTest {
    private String url= "hdfs://192.168.66.132:9000";
    private FileSystem fileSystem =  null;

    @Test
    public void mkdir()throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/a/b/c"));
    }

    @Test
    public void put() throws IOException {
        Path src = new Path("d:/logtest/xmiss-dailaimi.log");
        Path dst = new Path("/hdfsapi/a/b/c/xmiss-dailaimi.log");
        fileSystem.copyFromLocalFile(src,dst);
    }

    @Test
    public void ls() throws IOException {
        Path rootPath = new Path("/hdfsapi");
        printFilePath(rootPath);
    }

    public void printFilePath(Path path) throws IOException {
        FileStatus[] fss = fileSystem.listStatus(path);
        for(FileStatus fs : fss){
            System.out.println(fs.getPath().toString().replaceAll(url,"")+" "+fs.getReplication());

            if(fs.isDirectory()){
                printFilePath(fs.getPath());
            }
        }
    }

    @Test
    public void get() throws IOException {
        Path src = new Path("/hdfsapi/a/b/c/d1.txt");
        Path dst = new Path("d:/logtest/d2.txt");
        //delSrc是否删除源,src源,dst目标地址,是否本地文件(如果是false就是hdfs文件意思)
        fileSystem.copyToLocalFile(false,src,dst,true);
    }

    @Test
    public void rm() throws IOException {
        Path path = new Path("/hdfsapi/a/b/c/d1.txt");
        //true递归删除
        fileSystem.delete(path,true);
    }

    //启动
    @Before
    public void setUp() throws IOException, URISyntaxException, InterruptedException {
        URI uri =new URI(url);
        Configuration config = new Configuration();
        fileSystem = FileSystem.newInstance(uri,config,"root");
    }

    //结束
    @After
    public void tearDown() throws IOException {
        fileSystem.close();
    }
}
