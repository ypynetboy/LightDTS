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

package com.musichc.dts.lightdts.channel;

import com.musichc.dts.lightdts.arch.Channel;
import com.musichc.dts.lightdts.arch.Event;
import com.musichc.dts.lightdts.arch.lifecycle.AbstractLifecycle;
import com.musichc.dts.lightdts.arch.lifecycle.LifecycleState;

import java.util.LinkedList;
import java.util.Queue;

public class MemChannel extends AbstractLifecycle implements Channel {
    private Queue<Event> eventQueue;
    private int bufferSize = 50000;

    public MemChannel() {
        state = LifecycleState.IDLE;
        eventQueue = new LinkedList<Event>();
    }

    public MemChannel(int bufferSize) {
        super();
        this.bufferSize = bufferSize;
    }

    @Override
    public void put(Event event) {
        while ((eventQueue.size() > bufferSize)) {
            if (state != LifecycleState.START)
                break;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        eventQueue.add(event);
    }

    @Override
    public Event take() {
        Event e;
        while (true) {
            e = eventQueue.poll();
            // 获取到数据或非运行状态退出循环
            if (e != null || LifecycleState.START != state) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e1) {
            }
        }
        return e;
    }

    @Override
    public void stop() {
        eventQueue.clear();
        super.stop();
    }
}
