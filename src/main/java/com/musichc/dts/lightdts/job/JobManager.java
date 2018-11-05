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

import com.musichc.dts.lightdts.arch.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
public class JobManager {
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

    @Value("${dts.conf_dir}")
    private String confDir;

    @Value("${dts.concurrent_num}")
    private int concurrentNum;

    @Autowired
    private JobLoader jobLoader;

    private List<Job> jobs;

    @PostConstruct
    private void init() {
        try {
            jobs = jobLoader.load(confDir);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void start() {
        if (null == jobs)
            return;
        // TODO 控制任务同时执行数量
        for (Job job : jobs) {
            job.execute();
        }
    }
}
