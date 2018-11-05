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

package com.musichc.dts.lightdts.hbase.sink;

import com.google.common.base.Preconditions;
import com.musichc.dts.lightdts.arch.Channel;
import com.musichc.dts.lightdts.arch.Event;
import com.musichc.dts.lightdts.arch.Sink;
import com.musichc.dts.lightdts.arch.lifecycle.AbstractLifecycle;
import com.musichc.dts.lightdts.arch.lifecycle.LifecycleState;
import com.musichc.dts.lightdts.hbase.HBaseAdmin;
import com.musichc.dts.lightdts.hbase.HBaseConnection;
import com.musichc.dts.lightdts.hbase.write.HFamilyDesc;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.*;

public class HBaseSink extends AbstractLifecycle implements Runnable, Sink {
    private static final Logger logger = LoggerFactory.getLogger(HBaseSink.class);
    private static final int BULK_SIZE = 10000;

    private HBaseConnection connection;
    private HBaseAdmin admin;

    private String tableName;
    private HFamilyDesc[] familys;
    private HFamilyDesc defaultFamily;
    private RowKeyPattern rowKeyPattern;
    private Channel channel;

    private Map<String, byte[]> columnFamilyMap;
    private byte[] defaultFamilyName;

    public HBaseSink(HBaseConnection connection, HBaseAdmin admin, String tableName, HFamilyDesc[] familys, HFamilyDesc defaultFamily, RowKeyPattern rowKeyPattern) {
        Preconditions.checkNotNull(tableName, "tableName can not be null.");
        Preconditions.checkArgument(familys != null || defaultFamily != null, "familys or defaultFamily can not be null.");
        Preconditions.checkNotNull(rowKeyPattern, "rowKeyPattern can not be null.");
        this.connection = connection;
        this.admin = admin;
        this.tableName = tableName;
        this.familys = familys;
        this.defaultFamily = defaultFamily;
        this.rowKeyPattern = rowKeyPattern;
    }

    private void prestart() {
        // 生成字段与列簇对应表
        columnFamilyMap = new HashMap<>();
        if (familys != null) {
            for (HFamilyDesc family : familys) {
                byte[] familyName = Bytes.toBytes(family.getFamilyName());
                for (String column : family.getColumns()) {
                    columnFamilyMap.put(column, familyName);
                }
            }
        }
        // 接收通道数据
        defaultFamilyName = defaultFamily != null ? Bytes.toBytes(defaultFamily.getFamilyName()) : null;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(channel, "Channel can not be null.");
        // 初始化服务
        prestart();
        // 检查和创建表结构
        try {
            admin.createTable(tableName, familys);
        } catch (TableExistsException e) {
        } catch (IOException e) {
            logger.error("Create hbase table failed, Error message: {}", e.getMessage());
            return;
        }
        // 启动数据写入线程
        new Thread(this).start();
        super.start();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HFamilyDesc[] getFamilys() {
        return familys;
    }

    public void setFamilys(HFamilyDesc[] familys) {
        this.familys = familys;
    }

    public HFamilyDesc getDefaultFamily() {
        return defaultFamily;
    }

    public void setDefaultFamily(HFamilyDesc defaultFamily) {
        this.defaultFamily = defaultFamily;
    }

    public RowKeyPattern getRowKeyPattern() {
        return rowKeyPattern;
    }

    public void setRowKeyPattern(RowKeyPattern rowKeyPattern) {
        this.rowKeyPattern = rowKeyPattern;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void stop() {
        if (channel != null) {
            channel.stop();
        }
        super.stop();
    }

    @Override
    public void run() {
        Event event;
        try (Table table = connection.getTable(tableName)) {
            List<Put> puts = new ArrayList<>(BULK_SIZE);
            do {
                event = channel.take();
                if (event != null) {
                    Put put = newPut(event);
                    if (put != null) {
                        puts.add(put);
                    }
                }
                // 批量提交
                if (puts.size() >= BULK_SIZE) {
                    table.put(puts);
                    puts.clear();
                }
            } while (event != null && state == LifecycleState.START);
            // 提交最后的数据
            if (puts.size() > 0) {
                table.put(puts);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        stop();
    }

    private Put newPut(Event event) {
        Put put = new Put(Bytes.toBytes(rowKeyPattern.format(event)));
        for (String key : event.getKeys()) {
            byte[] familyName = columnFamilyMap.get(key);
            familyName = familyName != null ? familyName : defaultFamilyName;
            if (familyName != null) {
                try {
                    Object v = event.get(key);
                    if (v != null) {
                        put.addColumn(familyName, Bytes.toBytes(key), toBytes(v));
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                }
            }
        }
        return put;
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
            // TODO Test
            Blob obj1 = (Blob) obj;
            return obj1.getBytes(0, (int) obj1.length());
        } else if (obj instanceof Clob) {
            // TODO Test
            Clob clob = (Clob) obj;
            return Bytes.toBytes(clob.getSubString(0, 0));
        } else {
            return Bytes.toBytes(obj.toString());
        }
    }
}
