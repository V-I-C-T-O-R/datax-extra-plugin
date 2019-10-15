package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


//TODO writeProxy
public class KuduWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.KUDU;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private KuduWriter.KuduJob KuduJob;

        @Override
        public void preCheck(){
            this.init();
            this.KuduJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.KuduJob = new KuduWriter.KuduJob(DATABASE_TYPE);
            this.KuduJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.KuduJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.KuduJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.KuduJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.KuduJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private KuduWriter.KuduTask kuduTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.kuduTask = new KuduWriter.KuduTask(DATABASE_TYPE);
            this.kuduTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.kuduTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            this.kuduTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.kuduTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.kuduTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver(){
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }

    }

    public static class KuduTask {
        protected static final Logger LOG = LoggerFactory
                .getLogger(KuduWriter.KuduTask.class);

        protected DataBaseType dataBaseType;
        private static final String VALUE_HOLDER = "?";

        private static final Long DEFAULT_TIMEOUT_MILLIS =
                AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;
        private static final boolean DEFAULT_IGNORE_DUPLICATE_ROWS = true;
        private static final Integer DEFAULT_FLUSH_BUFFER_SIZE = 8000;
        private static final Long KUDU_TIMEOUT_MILLIS = 180000L;
        public static final long KUDU_DEFAULT_ADMIN_OPERATION_TIMEOUTMS = 180000L;
        public static final long KUDU_DEFAULT_OPERATION_TIMEOUTMS = 180000L;
        public static final long KUDU_DEFAULT_SOCKET_READ_TIMEOUTMS = 180000L;

        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected String primaryKey;
        protected List<String> columns;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;

        private KuduSession session;
        private KuduClient client;
        private String namespace;
        private long timeoutMillis;
        private int flushBufferSize;
        private boolean ignoreDuplicateRows;
        private KuduOperationsProducer operationsProducer;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;

        protected static String INSERT_OR_REPLACE_TEMPLATE;

        protected String writeRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        public KuduTask(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration writerSliceConfig) {
            int tableNumber = writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK,1);
            if (tableNumber == 1) {

                List<Object> conns = writerSliceConfig.getList(Constant.CONN_MARK,
                        Object.class);
                Configuration connConf = Configuration.from(conns.get(0)
                        .toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                writerSliceConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                writerSliceConfig.set(Key.TABLE, table);
            }

            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);

            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);
            this.table = writerSliceConfig.getString(Key.TABLE);
            this.namespace = writerSliceConfig.getString(Key.NAMESPACE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

            //新增
            this.primaryKey = writerSliceConfig.getString(Key.PRIMARYKEY);
            this.ignoreDuplicateRows = writerSliceConfig.getBool(Key.IGNORE_DUPLICATE_ROWS,DEFAULT_IGNORE_DUPLICATE_ROWS);
            this.flushBufferSize = writerSliceConfig.getInt(Key.FLUSH_BUFFER_SIZE,DEFAULT_FLUSH_BUFFER_SIZE);
            this.timeoutMillis = writerSliceConfig.getLong(Key.TIMEOUT_MILLIS,KUDU_TIMEOUT_MILLIS);

            this.operationsProducer = new JsonKuduOperationProducer();

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "upsert");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
//            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
//            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        public void prepare(Configuration writerSliceConfig) {
            if (client == null) {
                client = new KuduClient.KuduClientBuilder(this.jdbcUrl)
                        .defaultAdminOperationTimeoutMs(KUDU_DEFAULT_ADMIN_OPERATION_TIMEOUTMS)
                        .defaultOperationTimeoutMs(KUDU_DEFAULT_OPERATION_TIMEOUTMS)
                        .defaultSocketReadTimeoutMs(KUDU_DEFAULT_SOCKET_READ_TIMEOUTMS)
                        .build();
            }
            session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setTimeoutMillis(timeoutMillis);
            session.setIgnoreAllDuplicateRows(ignoreDuplicateRows);
            session.setMutationBufferSpace(flushBufferSize);

            String realTableName = null;
            if("".equals(namespace)){
                realTableName = this.table;
            }else{
                realTableName = namespace+"."+this.table;
            }
            operationsProducer.initialize(client,realTableName,this.primaryKey,this.columns,this.writeMode);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector) {
            this.taskPluginCollector = taskPluginCollector;

            List<Record> writeBuffer = new ArrayList<Record>(this.flushBufferSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException.asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }
                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= flushBufferSize || writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
            }
        }

        // TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver,
                               Configuration writerSliceConfig,
                               TaskPluginCollector taskPluginCollector) {
            startWriteWithConnection(recordReceiver, taskPluginCollector);
        }


        public void post(Configuration writerSliceConfig) {}

        public void destroy(Configuration writerSliceConfig) {
            try {
                operationsProducer.close();
            } catch (Exception e) {
                LOG.error("Error closing operations producer", e);
            }
            try {
                if(session != null){
                    session.close();
                }
                if (client != null) {
                    client.shutdown();
                }
                client = null;
                table = null;
            } catch (Exception e) {
                LOG.error("Error closing client", e);
            }
        }

        protected void doBatchInsert(List<Record> buffer) throws Exception {
            List<Operation> operations = this.operationsProducer.getOperations(buffer);
            for (Operation o : operations) {
                session.apply(o);
            }

            LOG.info("Flushing {} events", operations.size());
            List<OperationResponse> responses = session.flush();
            if (responses != null) {
                for (OperationResponse response : responses) {
                    if (response.hasRowError()) {
                        throw new Exception("Failed to flush one or more changes. " +
                                "Transaction rolled back: " + response.getRowError().toString());
                    }
                }
            }
        }

        protected void doOneInsert(Connection connection, List<Record> buffer) {
        }

    }

    public static class KuduJob {
        private DataBaseType dataBaseType;

        private static final Logger LOG = LoggerFactory
                .getLogger(CommonRdbmsWriter.Job.class);

        public KuduJob(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
            OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
        }

        public void init(Configuration originalConfig) {
            LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        /*目前只支持MySQL Writer跟Oracle Writer;检查PreSQL跟PostSQL语法以及insert，delete权限*/
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType) {

        }

        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType) {
        }

        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType) {
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        public void prepare(Configuration originalConfig) {
        }

        public List<Configuration> split(Configuration originalConfig,
                                         int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(originalConfig);
            }

            return writerSplitConfigs;
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig) {
        }

        public void destroy(Configuration originalConfig) {
        }

    }

}
