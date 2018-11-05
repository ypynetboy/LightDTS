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
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.musichc.dts.lightdts.hbase.HBaseAdmin;
import com.musichc.dts.lightdts.hbase.HBaseConnection;
import com.musichc.dts.lightdts.hbase.write.HFamilyDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class HBaseSinkLoader {
    @Autowired
    private HBaseConnection hBaseConnection;
    @Autowired
    private HBaseAdmin hBaseAdmin;

    public final HBaseSink load(JsonReader reader) throws IOException {
        String tableName = null;
        List<HFamilyDesc> familys = new ArrayList<>();
        HFamilyDesc defaultFamily = null;
        RowKeyPattern rowKeyPattern = null;

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("table_name") && reader.peek() != JsonToken.NULL) {
                tableName = reader.nextString();
            } else if (key.equals("familys") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    HFamilyDesc family = loadFamily(reader);
                    if (family != null) {
                        familys.add(family);
                    }
                }
                reader.endArray();
            } else if (key.equals("default_family") && reader.peek() == JsonToken.BEGIN_OBJECT) {
                defaultFamily = loadFamily(reader);
            } else if (key.equals("row_key") && reader.peek() == JsonToken.BEGIN_OBJECT) {
                rowKeyPattern = loadRowKey(reader);
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        Preconditions.checkNotNull(tableName, "table_name can not be null.");
        Preconditions.checkArgument(familys != null || defaultFamily != null, "familys or default_family can not be null.");
        Preconditions.checkNotNull(rowKeyPattern, "row_key can not be null.");
        return new HBaseSink(hBaseConnection, hBaseAdmin, tableName, (HFamilyDesc[]) familys.toArray(new HFamilyDesc[]{}),
                defaultFamily, rowKeyPattern);
    }

    private static final RowKeyPattern loadRowKey(JsonReader reader) throws IOException {
        String pattern = null;
        List<String> args = new ArrayList<>();
        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("pattern") && reader.peek() != JsonToken.NULL) {
                pattern = reader.nextString();
            } else if (key.equals("args") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    args.add(reader.nextString());
                }
                reader.endArray();
            }
        }
        reader.endObject();

        if (pattern != null) {
            return new RowKeyPattern(pattern, args);
        }
        return null;
    }

    private static final HFamilyDesc loadFamily(JsonReader reader) throws IOException {
        String familyName = null;
        Integer blocksize = null;
        List<String> columns = new ArrayList<>();

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("family_name") && reader.peek() != JsonToken.NULL) {
                familyName = reader.nextString();
            } else if (key.equals("block_size") && reader.peek() != JsonToken.NULL) {
                blocksize = reader.nextInt();
            } else if (key.equals("columns") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    columns.add(reader.nextString());
                }
                reader.endArray();
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        if (familyName != null) {
            HFamilyDesc result = new HFamilyDesc(familyName);
            if (blocksize != null) {
                result.setBlockSize(blocksize);
            }
            if (columns.size() > 0) {
                String[] arr = new String[columns.size()];
                result.addColumn(columns.toArray(arr));
            }
            return result;
        }
        return null;
    }
}
