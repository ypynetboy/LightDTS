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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.musichc.dts.lightdts.arch.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class JdbcSourceLoader {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Source load(JsonReader reader) throws IOException {
        String sql = null;
        List<String> args = new ArrayList<>();

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("sql") && reader.peek() != JsonToken.NULL) {
                sql = reader.nextString();
            } else if (key.equals("args") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    args.add(reader.nextString());
                }
                reader.endArray();
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        if (sql != null)
            return new JdbcSource(jdbcTemplate, sql, args);
        return null;
    }
}
