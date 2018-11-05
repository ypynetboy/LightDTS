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

public abstract class AbstractLifecycle implements Lifecycle {
    protected LifecycleState state = LifecycleState.IDLE;

    @Override
    public void start() {
        state = LifecycleState.START;
    }

    @Override
    public void stop() {
        state = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getState() {
        return state;
    }
}
