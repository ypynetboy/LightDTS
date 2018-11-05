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

package com.musichc.dts.lightdts.arch;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Event {
    private Map<String, Object> data;

    public Event() {
        data = new HashMap<>();
    }

    public Event(Map<String, Object> data) {
        this.data = data;
    }

    public Object get(String key) {
        if (data != null) {
            return data.get(key);
        }
        return null;
    }

    public Set<String> getKeys() {
        return data.keySet();
    }
}
