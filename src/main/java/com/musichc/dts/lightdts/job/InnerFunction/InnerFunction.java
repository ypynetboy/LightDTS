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

package com.musichc.dts.lightdts.job.InnerFunction;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class InnerFunction {
    public static final String FUNCTION_REGULAR = "^\\{.*}$";

    private enum FUNC_NAME {
        timestamp,
        long_timestamp,
        date,
        last_date,
        random_char
    }

    private static final char[] RANDOM_CHARS = new char[62];

    static {
        int index = 0;
        for (int i = 0; i < 10; i++) {
            RANDOM_CHARS[index++] = (char) ('0' + i);
        }
        for (int i = 0; i < 26; i++) {
            RANDOM_CHARS[index++] = (char) ('A' + i);
        }
        for (int i = 0; i < 26; i++) {
            RANDOM_CHARS[index++] = (char) ('a' + i);
        }
    }

    public static boolean isFunction(String param) {
        return param.matches(FUNCTION_REGULAR);
    }

    public static Object execute(String func) {
        String[] split = func.split(":");
        switch (FUNC_NAME.valueOf(split[0])) {
            case timestamp:
                return timestamp();
            case long_timestamp:
                return long_timestamp();
            case date:
                return date(new Date(), split.length > 1 ? split[1] : null);
            case last_date:
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DAY_OF_YEAR, -1);
                return date(calendar.getTime(), split.length > 1 ? split[1] : null);
            case random_char:
                return randomChar(split.length > 1 ? split[1] : null);
        }
        return null;
    }

    private static long timestamp() {
        return System.currentTimeMillis() / 1000;
    }

    private static long long_timestamp() {
        return System.currentTimeMillis();
    }

    private static String date(Date date, String arg) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(arg != null ? arg : "yyyy-MM-dd");
        return dateFormat.format(date);
    }

    private static String randomChar(String arg) throws NumberFormatException {
        if (null == arg)
            arg = "6";
        Integer n = Integer.valueOf(arg);
        Random random = new Random();
        StringBuilder result = new StringBuilder();
        for (Integer i = 0; i < n; i++) {
            result.append(RANDOM_CHARS[random.nextInt(RANDOM_CHARS.length)]);
        }
        return result.toString();
    }

    /*
    public static void main(String[] args) {
        System.out.println(execute("timestamp"));
        System.out.println(execute("long_timestamp"));
        System.out.println(execute("date"));
        System.out.println(execute("date:yyyyMMddHHmmss"));
        System.out.println(execute("last_date"));
        System.out.println(execute("random_char:8"));
    }*/
}
