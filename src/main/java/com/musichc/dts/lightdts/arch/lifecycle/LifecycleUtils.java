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

package com.musichc.dts.lightdts.arch.lifecycle;

public class LifecycleUtils {
    public static final int INTERVAL_TIME = 500;

    public static void waitFor(Lifecycle lifecycle) {
        while (lifecycle.getState() == LifecycleState.START) {
            try {
                Thread.sleep(INTERVAL_TIME);
            } catch (InterruptedException e) {
            }
        }
    }
}
