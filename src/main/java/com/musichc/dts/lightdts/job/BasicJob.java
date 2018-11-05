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
import com.musichc.dts.lightdts.arch.Channel;
import com.musichc.dts.lightdts.arch.Job;
import com.musichc.dts.lightdts.arch.Sink;
import com.musichc.dts.lightdts.arch.Source;
import com.musichc.dts.lightdts.arch.lifecycle.AbstractLifecycle;
import com.musichc.dts.lightdts.arch.lifecycle.LifecycleUtils;
import com.musichc.dts.lightdts.channel.MemChannel;

public class BasicJob extends AbstractLifecycle implements Job {
    private String id;
    private Source source;
    private Sink sink;
    private Channel channel;

    public BasicJob(String id, Source source, Sink sink) {
        this.id = id;
        this.source = source;
        this.sink = sink;
        this.channel = new MemChannel();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void execute() {
        Preconditions.checkNotNull(source, "Souce can not be null.");
        Preconditions.checkNotNull(sink, "Sink can not be null.");
        source.setChannel(channel);
        sink.setChannel(channel);
        // 启动
        channel.start();
        source.start();
        sink.start();
        // 等待结束
        LifecycleUtils.waitFor(source);
        LifecycleUtils.waitFor(sink);
        // 结束，清理
        channel.stop();
        super.stop();
    }
}
