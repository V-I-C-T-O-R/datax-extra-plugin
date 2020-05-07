package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class JsonKuduOperationProducer implements KuduOperationsProducer {

    private static final Logger logger = LoggerFactory.getLogger(JsonKuduOperationProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);
    public static final String DEFAULT_OPERATION = UPSERT;

    private String tableName;
    private KuduClient client;
    private String customKeys;
    private Charset charset;
    private String operation;
    private List<String> columns;
    public void JsonKuduOperationsProducer() {
    }

    @Override
    public void initialize(KuduClient kuduClient,String table,String customKeys,List<String> columns,String operation) {
        this.client = kuduClient;
        this.tableName = table;
        this.customKeys = customKeys;
        this.operation = operation;
        this.columns = columns;
    }
    @Override
    public List<Operation> getOperations(List<Record> records) throws Exception{
        List<Operation> ops = Lists.newArrayList();
        for(Record record : records){

            List<ColumnSchema> columns = new ArrayList();
            List<String> keys = Arrays.asList(this.customKeys.split(","));
            for(String key : keys){
                columns.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.STRING).key(true).nullable(false).build());
            }
            for(String columnStr : this.columns){
                if(keys.contains(columnStr))
                    continue;
                columns.add(new ColumnSchema.ColumnSchemaBuilder(columnStr, Type.STRING).key(false).nullable(true).build());
            }

            try{
                if (!this.client.tableExists(this.tableName)) {
                    Schema schema = new Schema(columns);
                    this.client.createTable(this.tableName, schema,new CreateTableOptions().setRangePartitionColumns(keys));
                }

                KuduTable kubuTable = this.client.openTable(this.tableName);
                Schema schema = kubuTable.getSchema();
                Operation op = null;
                switch (operation) {
                    case UPSERT:
                        op = kubuTable.newUpsert();
                        break;
                    case INSERT:
                        op = kubuTable.newInsert();
                        break;
                    default:
                        throw new Exception(
                                String.format("Unrecognized operation type '%s' in getOperations(): " +
                                        "this should never happen!", operation));
                }
                PartialRow row = op.getRow();
                for (ColumnSchema col : schema.getColumns()) {
                    try {
                        String colName = col.getName();
                        if(!this.columns.contains(colName))
                            continue;
                        Column columnObj = record.getColumn(this.columns.indexOf(colName));
                        String colValue = null;
                        if(columnObj.getType()==com.alibaba.datax.common.element.Column.Type.DATE){
                            Date tmpDate = columnObj.asDate();
                            if(tmpDate == null){
                                colValue = null;
                            }else{
                                colValue = String.valueOf(tmpDate.getTime());
                            }
                        } else if(columnObj.getType()== Column.Type.DOUBLE){
                            NumberFormat nf = NumberFormat.getInstance();
                            // 是否以逗号隔开, 默认true以逗号隔开,如[123,456,789.128]
                            nf.setGroupingUsed(false);
                            // 结果未做任何处理
                            colValue = nf.format(columnObj.asDouble());
                        } else{
                            colValue = String.valueOf(columnObj.asString());
                        }
                        coerceAndSet(colValue, colName, Type.STRING, row);
                    } catch (NumberFormatException e) {
                        String msg = String.format(
                                "Raw value '%s' couldn't be parsed to type %s for column '%s'",
                                record.toString(), col.getType(), col.getName());
                        logger.error(msg, e);
                    } catch (IllegalArgumentException e) {
                        String msg = String.format(
                                "Column '%s' has no matching group in '%s'",
                                col.getName(), schema.toString());
                        logger.error(msg, e);
                    } catch (Exception e) {
                        throw new Exception("Failed to create Kudu operation", e);
                    }
                }
                ops.add(op);
            }catch (KuduException e){
                e.printStackTrace();
            }

        }

        return ops;
    }
    /**
     * Coerces the string `rawVal` to the type `type` and sets the resulting
     * value for column `colName` in `row`.
     *
     * @param rawVal the raw string column value
     * @param colName the name of the column
     * @param type the Kudu type to convert `rawVal` to
     * @param row the row to set the value in
     * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
     */
    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(charset));
                break;
            case STRING:
                if(rawVal == null || "null".equals(rawVal)|| "".equals(rawVal)){
                    row.setNull(colName);
                }else{
                    row.addString(colName, rawVal);
                }
                break;
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case UNIXTIME_MICROS:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }
    }

    @Override
    public void close() {
    }
}
