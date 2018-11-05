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

import com.musichc.dts.lightdts.arch.Event;
import com.musichc.dts.lightdts.job.InnerFunction.InnerFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowKeyPattern {
    private String pattern;
    private List<String> args;

    public RowKeyPattern(String pattern, List<String> args) {
        this.pattern = pattern;
        this.args = args;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String format(Event event) {
        String format = pattern.replaceAll("\\?", "%s");
        List<String> params = new ArrayList<>();
        for (String arg : args) {
            if (InnerFunction.isFunction(arg)) {
                params.add(InnerFunction.execute(arg.substring(1, arg.length()-1)).toString());
            } else {
                Object value = event.get(arg);
                params.add(value != null ? value.toString() : "");
            }
        }
        return String.format(format, params.toArray());
    }

    public static void main(String[] args) {
        List<String> params = new ArrayList<>();
        params.add("{date}");
        params.add("id");
        RowKeyPattern pattern = new RowKeyPattern("id_?_date_?", params);
        Map<String, Object> row = new HashMap<>();
        row.put("id", 18741786348734L);
        System.out.println(pattern.format(new Event(row)));
    }
}
