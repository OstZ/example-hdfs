package cn.mysentiment.upload.dfs;

import java.util.List;

/**
 * HFDS操作接口
 */
public interface HDFSMgr {
    /**
     * 读取某个目录下的所有文件
     *
     * @param recursion 是否递归读取
     */
    List<String> ls(String path, boolean recursion);

    /**
     * 上传文件到指定位置
     * @param src 原始文件
     * @param dest 目标位置
     */
    void put(String src, String dest);

    /**
     * 从HDFS上下载文件到本地
     * @param src HDFS上的路径
     * @param destLocal 本地位置
     */
    void get(String src, String destLocal);

    /**
     * 创建目录
     * @param path 目标路径
     */
    void mkdir(String path);

    /**
     * 关闭操作HDFS的FileSystem
     */
    void close();
}