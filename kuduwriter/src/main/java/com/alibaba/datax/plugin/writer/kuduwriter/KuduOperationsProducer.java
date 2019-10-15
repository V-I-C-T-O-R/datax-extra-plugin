package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Record;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.Operation;

import java.util.List;

/**
 * Interface for an operations producer that produces Kudu Operations from
 * Flume events.
 */

public interface KuduOperationsProducer {
    /**
     * Initializes the operations producer. Called between configure and
     * getOperations.
     * @param kuduClient the KuduTable used to create Kudu Operation objects
     */
    void initialize(KuduClient kuduClient, String table, String customKeys,List<String> columns,String operation);

    List<Operation> getOperations(List<Record> record) throws Exception;

    /**
     * Cleans up any state. Called when the sink is stopped.
     */
    void close();
}
