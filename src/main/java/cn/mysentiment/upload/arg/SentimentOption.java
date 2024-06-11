package cn.mysentiment.upload.arg;
import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

import java.util.List;

public class SentimentOption extends OptionsBase {
    @Option(
            name = "help",
            abbrev = 'h',
            help = "print help info",
            defaultValue = "true"
    )
    public boolean help;

    @Option(
            name = "source",
            abbrev = 's',
            help = "要采集数据的位置",
            defaultValue = ""
    )
    public String sourceDir;

    @Option(
            name = "pending_dir",
            abbrev = 'p',
            help = "生成待上传的待上传目录",
            defaultValue = "/tmp/pending/sentiment"
    )
    public String pendingDir;

    @Option(
            name = "output",
            abbrev = 'o',
            help = "生成要上传到的HDFS路径",
            defaultValue = ""
    )
    public String output;

}
