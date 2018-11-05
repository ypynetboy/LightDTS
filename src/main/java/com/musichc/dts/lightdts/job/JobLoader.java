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

package com.musichc.dts.lightdts.job;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;
import com.musichc.dts.lightdts.arch.Job;
import com.musichc.dts.lightdts.arch.Sink;
import com.musichc.dts.lightdts.arch.Source;
import com.musichc.dts.lightdts.hbase.sink.HBaseSinkLoader;
import com.musichc.dts.lightdts.jdbc.source.JdbcSourceLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 任务加载器
 */
@Component
public class JobLoader {
    @Autowired
    private HBaseSinkLoader hBaseSinkLoader;

    @Autowired
    private JdbcSourceLoader jdbcSourceLoader;

    public List<Job> load(String confDir) throws IOException {
        List<Job> result = new ArrayList<>();
        File[] files = new File(confDir).listFiles((dir, name) -> name.endsWith(".dts"));
        if (files != null) {
            for (File file : files) {
                Job job = loadJobFile(file);
                result.add(job);
            }
        }
        return result;
    }

    private Job loadJobFile(File file) throws IOException {
        Source source = null;
        Sink sink = null;

        try (JsonReader reader = new JsonReader(new FileReader(file))) {
            reader.beginObject();
            while (reader.hasNext()) {
                String key = reader.nextName();
                if (key.equals("source")) {
                    source = readSource(reader);
                } else if (key.equals("sink")) {
                    sink = readSink(reader);
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        Preconditions.checkNotNull(source, "source can not be null.");
        Preconditions.checkNotNull(sink, "sink can not be null.");
        return new BasicJob(file.getAbsolutePath(), source, sink);
    }

    private Source readSource(JsonReader reader) throws IOException {
        return jdbcSourceLoader.load(reader);
    }

    private Sink readSink(JsonReader reader) throws IOException {
        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("hbase")) {
                return hBaseSinkLoader.load(reader);
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();
        return null;
    }
}
