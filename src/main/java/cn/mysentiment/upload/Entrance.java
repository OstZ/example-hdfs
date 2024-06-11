package cn.mysentiment.upload;

import cn.mysentiment.upload.arg.SentimentOption;
import cn.mysentiment.upload.task.TaskMgr;
import com.google.devtools.common.options.OptionsParser;
import jdk.internal.joptsimple.OptionParser;
import org.apache.log4j.Logger;

import java.util.Collections;

public class Entrance {
    protected static final Logger myLogger = Logger.getLogger(Entrance.class.getName());

    public static void main(String[] args) {
        OptionsParser parser = OptionsParser.newOptionsParser(SentimentOption.class);
        parser.parseAndExitUponError(args);
        SentimentOption options = parser.getOptions(SentimentOption.class);

        if(options.sourceDir.isEmpty() || options.output.isEmpty()){
            printUsage(parser);
            return;
        }
        myLogger.info("上报程序启动");

        TaskMgr taskMgr = new TaskMgr();

        myLogger.info("生成上传任务");
        taskMgr.genTask(options);

        myLogger.info("上传数据中");
        taskMgr.work(options);

        myLogger.info("DONE");

    }
    private static void printUsage(OptionsParser parser) {
        System.out.println("Usage: java -jar sentiment.jar OPTIONS");
        System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                OptionsParser.HelpVerbosity.LONG));
    }
}
