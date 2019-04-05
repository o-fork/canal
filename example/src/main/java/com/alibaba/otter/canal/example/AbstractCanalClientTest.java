package com.alibaba.otter.canal.example;

import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 测试基类
 *
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClientTest extends BaseCanalClientTest {

    public AbstractCanalClientTest(String destination){
        this(destination, null);
    }

    public AbstractCanalClientTest(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");

        thread = new Thread(() -> process());
        thread.setUncaughtExceptionHandler(handler);
        thread.start();

        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 1;

        while (running) {
            try {
                MDC.put("destination", destination);

                connector.connect();
                connector.subscribe();

                while (running) {
                    // 获取指定数量的数据
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();

                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    } else {
                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }

                    // 提交确认
                    connector.ack(batchId);
                    // 处理失败, 回滚数据
                    // connector.rollback(batchId);
                }


            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }


    }

}
