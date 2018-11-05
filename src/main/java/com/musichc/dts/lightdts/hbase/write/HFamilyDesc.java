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

package com.musichc.dts.lightdts.hbase.write;

import java.util.HashSet;
import java.util.Set;

public class HFamilyDesc {
    private String familyName;
    private int blockSize = 64 * 1024;
    private Set<String> columns = new HashSet<>();

    public HFamilyDesc(String familyName) {
        this.familyName = familyName;
    }

    public HFamilyDesc(String familyName, int blockSize) {
        this.familyName = familyName;
        this.blockSize = blockSize;
    }

    public HFamilyDesc addColumn(String columnName) {
        columns.add(columnName);
        return this;
    }

    public HFamilyDesc addColumn(String... columnsName) {
        for (String s : columnsName) {
            columns.add(s);
        }
        return this;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public HFamilyDesc setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }
}
