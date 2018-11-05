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

package com.musichc.dts.lightdts.jdbc.source;

import com.google.common.base.Preconditions;
import com.musichc.dts.lightdts.arch.Channel;
import com.musichc.dts.lightdts.arch.Event;
import com.musichc.dts.lightdts.arch.Source;
import com.musichc.dts.lightdts.arch.lifecycle.AbstractLifecycle;
import com.musichc.dts.lightdts.arch.lifecycle.LifecycleState;
import com.musichc.dts.lightdts.job.InnerFunction.InnerFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSource extends AbstractLifecycle implements Source, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);
    private String sql;
    private List<String> args;
    private List<String> columns;
    private Channel channel;

    private JdbcTemplate jdbcTemplate;

    public JdbcSource(JdbcTemplate jdbcTemplate, String sql, List<String> args) {
        this.jdbcTemplate = jdbcTemplate;
        this.sql = sql;
        this.args = args;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(channel, "Channel can not be null.");
        new Thread(this).start();
        super.start();
    }

    @Override
    public void run() {
        List<Object> params = new ArrayList<>(args.size());
        try {
            for (String arg : args) {
                if (InnerFunction.isFunction(arg)) {
                    params.add(InnerFunction.execute(arg.substring(1, arg.length() - 1)));
                }
            }
            jdbcTemplate.query(sql, params.toArray(), new ResultSetExtractor<Object>() {
                @Override
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    List<String> columns = new ArrayList<>(metaData.getColumnCount());
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        columns.add(metaData.getColumnName(i));
                    }
                    JdbcSource.this.columns = columns;

                    while (resultSet.next()) {
                        Map<String, Object> row = readRow(resultSet);
                        channel.put(new Event(row));
                        if (channel.getState() != LifecycleState.START)
                            break;
                    }
                    stop();
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error(e.getMessage());
            stop();
            return;
        }
    }

    private Map<String, Object> readRow(ResultSet resultSet) throws SQLException {
        Map<String, Object> result = new HashMap<>(columns.size());
        for (String column : columns) {
            Object value = resultSet.getObject(column);
            if (value != null) {
                result.put(column, value);
            }
        }
        return result;
    }

    @Override
    public void stop() {
        if (channel != null) {
            channel.stop();
        }
        super.stop();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

}
