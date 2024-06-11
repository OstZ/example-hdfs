package cn.myhdfs;

import org.apache.log4j.Logger;

public class log4jTest {
    public static void main(String[] args) {
        Logger mylogger = Logger.getLogger(log4jTest.class);
        mylogger.error("got error");
        mylogger.warn("got warn");
        mylogger.fatal("got fatal");
        mylogger.info("got info");
        mylogger.debug("got debug");
    }
}
