package com.alibaba.datax.plugin.reader.ImpalaReader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class ImpalaReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Impala;
    private static final Integer FETCH_SIZE_DEFAULT = 1000;
    private static DruidDataSource datasource;
    private static String querySql;
    private static final String KEY_DATASOURCE = "datasource";

    private static DruidDataSource getDataSource() {
        return ImpalaReader.datasource;
    }

    private static void setDataSource(DruidDataSource datasource) {
        ImpalaReader.datasource = datasource;
    }

    private static String getQuerySql() {
        return querySql;
    }

    private static void setQuerySql(String querySql) {
        ImpalaReader.querySql = querySql;
    }

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
            Integer fetchSize = null;
            if (userConfigedFetchSize == null) {
                fetchSize = FETCH_SIZE_DEFAULT;
            } else {
                fetchSize = userConfigedFetchSize;
            }
            LOG.info("Custom parameter [ fetchSize ] = {}", fetchSize.toString());

            this.originalConfig.set(Constant.FETCH_SIZE, fetchSize);

            ImpalaJob impalaJob = new ImpalaReader.ImpalaJob();
            impalaJob.init(this.originalConfig);
        }

        @Override
        public void preCheck() {
            init();
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }
            return writerSplitConfigs;
        }
    }

    public static class ImpalaJob {
        private static final Logger logger = LoggerFactory.getLogger(ImpalaJob.class);
        private String username = "username";
        private String password = "password";//暂未用到

        private String initialSize = "initialSize";
        private String minIdle = "minIdle";
        private String maxActive = "maxActive";
        private String maxWait = "maxWait";
        private String timeBetweenEvictionRunsMillis = "timeBetweenEvictionRunsMillis";
        private String minEvictableIdleTimeMillis = "minEvictableIdleTimeMillis";
        private String validationQuery = "validationQuery";
        private String testWhileIdle = "testWhileIdle";
        private String testOnBorrow = "testOnBorrow";
        private String testOnReturn = "testOnReturn";
        private String poolPreparedStatements = "poolPreparedStatements";
        private String maxPoolPreparedStatementPerConnectionSize = "maxPoolPreparedStatementPerConnectionSize";
        private String useGlobalDataSourceStat = "useGlobalDataSourceStat";
        private String removeAbandoned = "removeAbandoned";
        private String removeAbandonedTimeout = "removeAbandonedTimeout";
        private String logAbandoned = "logAbandoned";

        public void init(Configuration originalConfig) {
            DruidDataSource datasource = ImpalaReader.getDataSource();
            if (datasource != null)
                return;

            List<Object> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.writer.Constant.CONN_MARK,
                    Object.class);
            Configuration connConf = Configuration.from(conns.get(0)
                    .toString());
            //检查必要参数
            originalConfig.getNecessaryValue(ImpalaReader.KEY_DATASOURCE, DBUtilErrorCode.REQUIRED_VALUE);
            connConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);
            Configuration dataSourceConf = Configuration.from(originalConfig.get(ImpalaReader.KEY_DATASOURCE)
                    .toString());

            String querySql = connConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL);
            //不支持splitPK
            List<String> columns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Key.COLUMN, String.class);
            if (null != columns && null != querySql)
                connConf.getNecessaryValue(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL, DBUtilErrorCode.TABLE_QUERYSQL_MISSING);
            String where = originalConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.WHERE, null);
            String column = StringUtils.join(columns, ",");
            logger.info(String.format("jdbcUrl:[%s]", connConf.getString(Key.JDBC_URL)));

            datasource = new DruidDataSource();
            datasource.setUrl(connConf.getString(Key.JDBC_URL));
            datasource.setUsername(dataSourceConf.getString(this.username));
            datasource.setPassword(dataSourceConf.getString(this.password));
            datasource.setDriverClassName(ImpalaReader.DATABASE_TYPE.getDriverClassName());

            //configuration
            datasource.setInitialSize(dataSourceConf.getInt(this.initialSize));
            datasource.setMinIdle(dataSourceConf.getInt(this.minIdle));
            datasource.setMaxActive(dataSourceConf.getInt(this.maxActive));
            datasource.setMaxWait(dataSourceConf.getInt(this.maxWait));
            datasource.setTimeBetweenEvictionRunsMillis(dataSourceConf.getInt(this.timeBetweenEvictionRunsMillis));
            datasource.setMinEvictableIdleTimeMillis(dataSourceConf.getInt(this.minEvictableIdleTimeMillis));
            datasource.setValidationQuery(dataSourceConf.getString(this.validationQuery));
            datasource.setTestWhileIdle(dataSourceConf.getBool(this.testWhileIdle));
            datasource.setTestOnBorrow(dataSourceConf.getBool(this.testOnBorrow));
            datasource.setTestOnReturn(dataSourceConf.getBool(this.testOnReturn));
            datasource.setPoolPreparedStatements(dataSourceConf.getBool(this.poolPreparedStatements));
            datasource.setMaxPoolPreparedStatementPerConnectionSize(dataSourceConf.getInt(this.maxPoolPreparedStatementPerConnectionSize));
            datasource.setUseGlobalDataSourceStat(dataSourceConf.getBool(this.useGlobalDataSourceStat));
            datasource.setRemoveAbandoned(dataSourceConf.getBool(this.removeAbandoned));
            datasource.setRemoveAbandonedTimeout(dataSourceConf.getInt(this.removeAbandonedTimeout));
            datasource.setLogAbandoned(dataSourceConf.getBool(this.logAbandoned));
            ImpalaReader.setDataSource(datasource);
            ImpalaReader.setQuerySql(buildQuerySql(connConf, column, where));
        }

        private String buildQuerySql(Configuration connConf, String column, String where) {
            String querys = null;
            String isTableMode = connConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE);

            // 说明是配置的 table 方式
            if (isTableMode != null && !isTableMode.trim().isEmpty()) {
                // 已在之前进行了扩展和`处理，可以直接使用
                String table = connConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE, String.class).get(0);
                Validate.isTrue(null != table && !table.isEmpty(), "您读取数据库表配置错误.");

                querys = SingleTableSplitUtil.buildQuerySql(column, table, where);
            } else {
                // 说明是配置的 querySql 方式
                querys = connConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL);
            }
            return querys;
        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration readerSliceConfig;
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);
            this.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        private void startRead(Configuration readerSliceConfig,
                               RecordSender recordSender,
                               TaskPluginCollector taskPluginCollector, int fetchSize) {
            String querySql = ImpalaReader.getQuerySql();
            String table = readerSliceConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE);
            String mandatoryEncoding = readerSliceConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.MANDATORY_ENCODING, "");
            PerfTrace.getInstance().addTaskDetails(super.getTaskId(), table);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql);
            PerfRecord queryPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = null;
            int columnNumber = 0;
            ResultSet rs = null;
            try {
                conn = ImpalaReader.getDataSource().getConnection();
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n].",
                        querySql);

            } catch (Exception e) {
                throw RdbmsException.asQueryException(DATABASE_TYPE, e, querySql, table, "");
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        private Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                          ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                          TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        private Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                   TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                        rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            DBUtilErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                LOG.warn("出现异常数据:" + record.toString(), e);
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            DruidDataSource datasource = ImpalaReader.getDataSource();
            if (datasource != null && datasource.isKeepAlive())
                datasource.close();

        }
    }
}
