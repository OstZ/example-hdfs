package cn.mysentiment.upload.dfs;
import jdk.internal.net.http.common.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HDFSMgrImpl implements HDFSMgr{
    protected static Logger Logger = LogManager.getLogger(HDFSMgrImpl.class.getName());
    //Conf and fs
    private Configuration conf = null;
    private FileSystem fs = null;

    public HDFSMgrImpl(){
        conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public List<String> ls(String path, boolean recursion){
        List<String> files = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), recursion);
            while(listFiles.hasNext()){
                files.add(listFiles.next().getPath().toString());
            }
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            return files;
        }
    }


    @Override
    public void put(String src, String dest) {
        Path srcPh = new Path(src);
        Path dstPh = new Path(dest);
        //if not exists throw error
        try {
            if(!fs.exists(srcPh))
                throw new IOException("no such file");
            fs.copyFromLocalFile(false, true, srcPh, dstPh);
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void get(String src, String destLocal) {
        Path srcPh = new Path(src);
        Path dstPh = new Path(destLocal);
        //if not exists throw error
        try {
            if(!fs.exists(srcPh))
                throw new IOException("no such file");
            fs.copyToLocalFile(srcPh, dstPh);
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void mkdir(String path) {
        //if exists
        Path newPh = new Path(path);
        try {
            if(fs.exists(newPh)) return;
            fs.mkdirs(newPh);
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
