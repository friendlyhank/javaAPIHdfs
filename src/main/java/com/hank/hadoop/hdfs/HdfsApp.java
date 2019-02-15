package com.hank.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;



public class HdfsApp {
    // log4j对象，用于收集日志
    private static Logger logger = Logger.getLogger("file");
    //存放遍历出来的HDFS路径
    private static Set<String> set = new HashSet<String>();

    public FileSystem getHadoopFileSystem(String uri,String userName){
        FileSystem fs = null;
        Configuration conf = null;

        // 方法二：本地没有hadoop系统，但是可以远程访问。根据给定的URI和用户名，访问hdfs的配置参数
        // 此时的conf不需任何设置，只需读取远程的配置文件即可。

        //远程节点的URL
        URI hdfsUrl = null;
        conf = new Configuration();

        try{
            hdfsUrl = new URI(uri);
        }catch(URISyntaxException e){
            e.printStackTrace();
            logger.error(e);
        }

        try{
            //userName Hadoop用户名
            fs = FileSystem.get(hdfsUrl,conf,userName);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);
        }

        return fs;
    }

    /**
     *获取配置的所有信息
     *配置信息可以用迭代器接收
     * 实际配置是KV对，我们可以用java中的Entry来接收
     */
    public void showAllConf(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.66.131:9000");
        //map的迭代器，用于遍历map的每一个键值对
        Iterator<Map.Entry<String,String>> it  = conf.iterator();
        while (it.hasNext()){
            Map.Entry<String,String> entry = it.next();
            System.out.println(entry.getKey()+"="+entry.getValue());
        }
    }

    /**
     * 检查路径，目录，或文件
     * @param fs
     * @param dirPath
     */
    public void myCheck(FileSystem fs,String dirPath){
        boolean isExists = false;
        boolean isDirectorys = false;
        boolean isFiles = false;

        Path path = new Path(dirPath);

        try{
            isExists = fs.exists(path);
            isDirectorys = fs.isDirectory(path);
            isFiles = fs.isFile(path);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                fs.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }

        if(!isExists){
            System.out.println("path not found");
        }else{
            System.out.println("path is founded");
            if(isDirectorys){
                System.out.println("Directory");
            }else if(isFiles){
                System.out.println("Files");
            }
        }
    }


    /**
     *创建文件夹
     * 跟Java中的IO操作一样，也只能对path对象操作；但是这里Path对象是hdfs中的
     * @param fs
     * @param dirPath
     * @return
     */
    public boolean myCreatePath(FileSystem fs,String dirPath){
        boolean b= false;

        Path path = new Path(dirPath);
        try{
            //even the path exist,it can also create the path
            b =fs.mkdirs(path);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                fs.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }
        return b;
    }

    /**
     * 遍历文件夹
     * @param hdfs
     * @param listPath
     * @return
     */
    public Set<String>recursiveHdfsPath(FileSystem hdfs,Path listPath){
        FileStatus[] files = null;
        try{
            files = hdfs.listStatus(listPath);
            //实际上并不是每个文件夹都会有文件的
            if(files.length == 0){
                // 如果不使用toUri()，获取的路径带URL。
                set.add(listPath.toUri().getPath());
            }else {
                //判断是否为文件
                for(FileStatus f: files){
                    if(files.length == 0 || f.isFile()){
                        set.add(f.getPath().toUri().getPath());
                    }else{
                        //递归
                        recursiveHdfsPath(hdfs,f.getPath());
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }
        return set;
    }

    /**
     * 修改目录,文件
     * @param hdfs
     * @param oPath
     * @param nPath
     * @return
     */
    public boolean myRename(FileSystem hdfs,String oPath,String nPath){
        boolean b = false;
        Path oldPath = new Path(oPath);
        Path newPath = new Path(nPath);

        try{
            b = hdfs.rename(oldPath,newPath);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                hdfs.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }

        return b;
    }

    /**
     * 复制文件
     * @param fs
     * @param iPath
     * @param oPath
     */
    public void copyFileBetweenHDFS(FileSystem fs,String iPath,String oPath){
        Path inPath = new Path(iPath);
        Path outPath = new Path(oPath);

        //HDFS输入输出流
        FSDataInputStream hdfsIn = null;
        FSDataOutputStream hdfsOut =  null;

        try{
            hdfsIn = fs.open(inPath);
            hdfsOut = fs.create(outPath);

            //Hadoop的流复制
            IOUtils.copyBytes(hdfsIn,hdfsOut,1024*1024*64,false);
        }catch(IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                hdfsIn.close();
                hdfsOut.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }
    }

    /**
     * 下载文件到本地
     * @param fs
     * @param hdfsPath
     * @param localPath
     * @return
     */
    public void getFileFromHdfs(FileSystem fs,String hPath,String lPath){
        Path localPath = new Path(lPath);
        Path HdfsPath = new Path(hPath);

        try{
            fs.copyToLocalFile(HdfsPath,localPath);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                fs.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }
    }

    /**
     * 上传文件
     * @param fs
     * @param localPath
     * @return
     */
    public void myPutFileHdfs(FileSystem fs,String lPath,String hPath){
            Path localPath = new Path(lPath);
            Path HdfsPath = new Path(hPath);

            try{
                fs.copyFromLocalFile(localPath,HdfsPath);
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }finally {
                try {
                    fs.close();
                }catch (IOException e){
                    e.printStackTrace();
                    logger.error(e);
                }
            }
    }


    /**
     * 删除文件
     * @param fs
     * @param dirPath
     * @return
     */
    public boolean myDropHdfsPath(FileSystem fs,String dirPath){
        boolean b=false;
        //drop the last path
        Path path = new Path(dirPath);
        try{
            b = fs.delete(path,true);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e);
        }finally {
            try{
                fs.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }
        return b;
    }


    public static void main(String []args){
        HdfsApp hdfs = new HdfsApp();
        FileSystem fs = hdfs.getHadoopFileSystem("hdfs://192.168.66.131:9000","root");

        //输出配置信息
//        hdfs.showAllConf();

        //路径,目录,文件判断
//        hdfs.myCheck(fs,"testpatch/xmiss-dailaimi.log");

        //创建hdfs目录
//        hdfs.myCreatePath(fs,"/user/root/hdfstest");

        //修改目录
//        hdfs.myRename(fs,"testpatch","textpatch");

        //遍历文件夹
//        Set<String> set = hdfs.recursiveHdfsPath(fs,new Path("textpatch/"));
//        //放到Set集合后遍历输出
//        for(String path:set){
//            System.out.println(path);
//        }

        //修改文件
//        hdfs.myRename(fs,"textpatch/xmiss-dailaimi.log","textpatch/xmissdailaimi.log");

        //上传文件(如果只有一个节点会报错为啥？)
        //File /user/root/testpatch/xmissmc.log could only be replicated to 0 nodes instead of minReplication (=1).  There are 1 datanode(s) running and 1 node(s) are excluded in this operation
//        hdfs.myPutFileHdfs(fs,"d:/logtest/xmissmc.log","testpatch/");

        //下载文件
        //1.error  HADOOP_HOME and hadoop.home.dir are unset
        //如果是window中跑代码要安装Hadoop，下载文件,配置完要重启
        //2.配置完之后依然会报错
        //Could not obtain block: BP-905971608-127.0.0.1-1549872311849:blk_1073741871_1047 file=/user/root/wordcount/input/xmiss-dailaimi.log
//        hdfs.getFileFromHdfs(fs,"wordcount/input/xmiss-dailaimi.log","d:/logtest/logdown");

        //复制文件
//        hdfs.copyFileBetweenHDFS(fs,"textpatch/xmissdailaimi.log","hdfstest/xmissdailaimi.log");

        //删除文件
//        hdfs.myDropHdfsPath(fs,"");


    }
}
