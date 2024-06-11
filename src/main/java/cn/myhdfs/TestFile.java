package cn.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestFile {
    private static FileSystem fs = null;
    private static Configuration conf = null;

    /**
     * connect to hdfs
     * @throws IOException
     */
    @Before
    public void createFS() throws IOException {
        //设置客户端权限,以便在HDFS上执行相应操作
        System.setProperty("HADOOP_USER_NAME", "root");

        conf = new Configuration();
        //设置文件系统(k-v)，指定HDFS操作地址，结点和端口
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //返回datanode的主机名而不是ip
        //conf.set("dfs.client.use.datanode.hostname", "true");
        fs = FileSystem.get(conf);
    }
    @Test
    public void mkdir() throws IOException {
        int flag = 0;
        if(!fs.exists(new Path("/source/java_api"))){
            fs.mkdirs(new Path("/source/java_api"));
        }
    }
    @Test
    public void upload() throws IOException {
        Path src = new Path("/home/zsm/CS/NOTE/note/HDFSArchitecture.png");
        Path dst = new Path("/source/java_api");
        fs.copyFromLocalFile(src, dst);
    }
    @Test
    public void download() throws IOException {
        Path src = new Path("/source/java_api/big.txt");
        Path dst = new Path("/home/zsm/CS/NOTE/note");
        fs.copyToLocalFile(src, dst);
    }

    /**
     * close connection
     */
    @After
    public void close(){
        if(fs != null){
            try {
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
