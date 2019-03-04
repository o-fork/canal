package com.alibaba.otter.canal.example.db;

import com.alibaba.otter.canal.example.db.mysql.MysqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlLoadLauncher {

    private static final Logger logger = LoggerFactory.getLogger(MysqlLoadLauncher.class);

    public static void main(String[] args) {

        logger.info("## start the canal mysql client.");
        final MysqlClient client = ServiceLocator.getMysqlClient();
        logger.info("## the canal consumer is running now ......");
        client.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal consumer");
                client.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal consumer:\n{}", e);
            } finally {
                logger.info("## canal consumer is down.");
            }
        }));

    }
}
