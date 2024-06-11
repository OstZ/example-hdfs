package cn.mysentiment.upload.task;

import cn.mysentiment.upload.arg.SentimentOption;
import cn.mysentiment.upload.dfs.HDFSMgr;
import cn.mysentiment.upload.dfs.HDFSMgrImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;

public class TaskMgr {
    protected static Logger Logger = LogManager.getLogger(TaskMgr.class.getName());

    protected static final String COPY_STATUS = "_COPY";
    protected static final String DONE_STATUS = "_DONE";

    private HDFSMgr hdfsUtil;

    public TaskMgr(){
        hdfsUtil = new HDFSMgrImpl();
    }

    public void genTask(SentimentOption options){
        //数据采集
        //1.sourceDir exists or not
        File sourceDir = new File(options.sourceDir);
        if(!sourceDir.exists()){
            String errMsg = String.format("%s source data dir not exits", sourceDir.getPath());
            Logger.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        //2.read files
        File[] allSourceDataFile = sourceDir.listFiles(f->{
            String fileName = f.getName();
            if(fileName.startsWith("weibo_data_")){
                return true;
            }
            return false;
        });

        //upload dir exists or not, if not exists, create a new dir
        File pendingDir = new File(options.pendingDir);
        if(!pendingDir.exists()){
            try {
                FileUtils.forceMkdir(pendingDir);
            } catch (IOException e) {
                Logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        //create String formator
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        StringBuilder sb = new StringBuilder();

        //创建任务目录（目录名称：task_年月日时分秒_任务状态
        File taskDir = null;
        if(allSourceDataFile != null && allSourceDataFile.length > 0){
            taskDir = new File(pendingDir, String.format("task_%s", sdf.format(new Date())));
            taskDir.mkdir();
        } else {
            return;
        }
        // 遍历待上传的文件
        // 在待上传目录生成一个willDoing文件,记录每个要上传文件的绝对路径
        for(File dataFile:allSourceDataFile){
            try{
                File dstFile = new File(taskDir, dataFile.getName());
                //每个文件复制到task目录下
                FileUtils.moveFile(dataFile, dstFile);
                //每个文件一行
                sb.append(dstFile.getAbsoluteFile() + "\n");
            } catch (IOException e){
                Logger.error(e.getMessage(), e);
            }
        }
        //将待上传目录写入到文件中
        try{
            String taskName = String.format("willDoing_%s",sdf.format(new Date()));
            FileUtils.writeStringToFile(new File(pendingDir, taskName),
                    sb.toString(),
                    "utf-8");
        } catch (IOException e){
            Logger.error(e.getMessage());
        }

    }

    public void work(SentimentOption options){
        //读取并选择符合上传条件的文件
        File pendingDir = new File(options.pendingDir);
        File[] pendingTaskDir = pendingDir.listFiles(f ->{
            //排除目录
            if(!f.getName().startsWith("willDoing")) return false;
            //排除正在上传和已上传文件
            if(f.getName().endsWith(COPY_STATUS) || f.getName().endsWith(DONE_STATUS)) return false;
            else return true;
        });

        //遍历读取任务文件，开始上传
        for (File pendingTask : pendingTaskDir) {
            try {
                // 将任务文件修改为_COPY，表示正在处理中
                File copyTaskFile = new File(pendingTask.getAbsolutePath() + "_" + COPY_STATUS);
                FileUtils.moveFile(pendingTask, copyTaskFile);

                // 获取任务的日期
                String taskDate = pendingTask.getName().split("_")[1];
                String dataPathInHDFS = options.output + String.format("/%s", taskDate);
                // 判断HDFS目标上传目录是否存在，不存在则创建
                hdfsUtil.mkdir(dataPathInHDFS);

                // 读取任务文件
                String tasks = FileUtils.readFileToString(copyTaskFile, "utf-8");
                // 按照换行符切分
                String[] taskArray = tasks.split("\n");

                // 上传每一个文件
                for (String task : taskArray) {
                    // 调用HDFSUtils进行数据文件上传
                    hdfsUtil.put(task, dataPathInHDFS);
                }

                // 上传成功后，将_COPY后缀修改为_DONE
                File doneTaskFile = new File(pendingTask.getAbsolutePath() + "_" + DONE_STATUS);
                FileUtils.moveFile(copyTaskFile, doneTaskFile);
            } catch (IOException e) {
                Logger.error(e.getMessage(), e);
            }

        }
    }


}
