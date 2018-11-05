/*
 * Copyright (c) 2018. yangpy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Author: yangpy
 * Created: 18-11-5 下午7:33
 */

package com.musichc.dts.lightdts.hbase.write;

import com.musichc.dts.lightdts.hbase.HBaseConnection;
import com.musichc.dts.lightdts.utils.Callback;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
@Scope("prototype")
public class HBaseTableImporter {
    private static final int BULK_SIZE = 10000;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private HBaseConnection hbaseConnection;

    private Callback<Integer> resultCallback;

    private Callback<Exception> exceptionCallback;

    public void setResultCallback(Callback<Integer> resultCallback) {
        this.resultCallback = resultCallback;
    }

    public void setExceptionCallback(Callback<Exception> exceptionCallback) {
        this.exceptionCallback = exceptionCallback;
    }

    /**
     * 数据导入HBase
     *
     * @param sql           数据源SQL
     * @param tableName     HBase表名
     * @param familys       列簇信息
     * @param rowKeyBuilder rowKey生成器
     */
    public void doImport(String sql, String tableName, HFamilyDesc[] familys, RowKeyBuilder rowKeyBuilder) {
        jdbcTemplate.query(sql, new MyResultSetExtractor(tableName, familys, rowKeyBuilder));
    }

    /**
     * 数据导入HBase
     *
     * @param sql           数据源SQL
     * @param args          SQL参数
     * @param tableName     HBase表名
     * @param familys       列簇信息
     * @param rowKeyBuilder rowKey生成器
     */
    public void doImport(String sql, Object[] args, String tableName, HFamilyDesc[] familys, RowKeyBuilder rowKeyBuilder) {
        jdbcTemplate.query(sql, args, new MyResultSetExtractor(tableName, familys, rowKeyBuilder));
    }

    /**
     * 数据导入HBase
     *
     * @param sql           数据源SQL
     * @param args          SQL参数
     * @param argTypes      SQL参数类型
     * @param tableName     HBase表名
     * @param familys       列簇信息
     * @param rowKeyBuilder rowKey生成器
     */
    public void doImport(String sql, Object[] args, int[] argTypes, String tableName, HFamilyDesc[] familys, RowKeyBuilder rowKeyBuilder) {
        jdbcTemplate.query(sql, args, argTypes, new MyResultSetExtractor(tableName, familys, rowKeyBuilder));
    }

    /**
     * 遍历数据，生成HBASE格式数据，存储到HBASE
     */
    private class MyResultSetExtractor implements ResultSetExtractor<Object> {
        private String tableName;
        private HFamilyDesc[] familys;
        private RowKeyBuilder rowKeyBuilder;

        public MyResultSetExtractor(String tableName, HFamilyDesc[] familys, RowKeyBuilder rowKeyBuilder) {
            this.tableName = tableName;
            this.familys = familys;
            this.rowKeyBuilder = rowKeyBuilder;
        }

        @Override
        public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
            long startTime = System.currentTimeMillis();
            long count = 0;
            // TODO 利用元数据检查列簇字段是否存在
            try (Table table = hbaseConnection.getTable(tableName)) {
                List<Put> puts = new ArrayList<>(BULK_SIZE);
                while (resultSet.next()) {
                    Put put = new Put(Bytes.toBytes(rowKeyBuilder.getRowKey(resultSet)));
                    for (HFamilyDesc family : familys) {
                        byte[] familyName = Bytes.toBytes(family.getFamilyName());
                        for (String column : family.getColumns()) {
                            Object obj = resultSet.getObject(column);
                            if (obj != null) {
                                put.addColumn(familyName, Bytes.toBytes(column), toBytes(obj));
                            }
                        }
                    }
                    puts.add(put);
                    if (puts.size() >= BULK_SIZE) {
                        table.put(puts);
                        puts.clear();
                    }
                    // TEST
                    if (++count % 5000 == 0) {
                        System.out.printf("Load %d waste %dms, %dops\r\n", count, System.currentTimeMillis() - startTime,
                                count / Math.max((System.currentTimeMillis() - startTime) / 1000, 1));
                    }
                }
                System.out.println("Load EMPI data waste: " + (System.currentTimeMillis() - startTime) + "ms");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        // 数据库Value转byte数组
        private byte[] toBytes(Object obj) throws SQLException {
            if (obj instanceof String) {
                return Bytes.toBytes((String) obj);
            } else if (obj instanceof BigDecimal) {
                return Bytes.toBytes((BigDecimal) obj);
            } else if (obj instanceof Boolean) {
                return Bytes.toBytes((Boolean) obj);
            } else if (obj instanceof Byte) {
                return Bytes.toBytes((Byte) obj);
            } else if (obj instanceof Long) {
                return Bytes.toBytes((Long) obj);
            } else if (obj instanceof Integer) {
                return Bytes.toBytes((Integer) obj);
            } else if (obj instanceof Short) {
                return Bytes.toBytes((Short) obj);
            } else if (obj instanceof Double) {
                return Bytes.toBytes((Double) obj);
            } else if (obj instanceof Float) {
                return Bytes.toBytes((Float) obj);
            } else if (obj instanceof byte[]) {
                return (byte[]) obj;
            } else if (obj instanceof Date) {
                return Bytes.toBytes(((Date) obj).getTime());
            } else if (obj instanceof Blob) {
                Blob obj1 = (Blob) obj;
                return obj1.getBytes(0, (int) obj1.length());
            } else if (obj instanceof Clob) {
                Clob clob = (Clob) obj;
                return Bytes.toBytes(clob.getSubString(0, 0));
            } else {
                return Bytes.toBytes(obj.toString());
            }
        }
    }

    private class DefRowKeyBuilder implements RowKeyBuilder {
        private ResultSet resultSet;

        @Override
        public String getRowKey(ResultSet rs) throws SQLException {
            // 判断是否已获得主键
            if (rs != resultSet) {
                resultSet = rs;
                ResultSetMetaData metaData = rs.getMetaData();
            }
            return null;
        }
    }

    ;
}
