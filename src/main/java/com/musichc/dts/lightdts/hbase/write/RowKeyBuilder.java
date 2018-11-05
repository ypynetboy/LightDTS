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

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowKeyBuilder {
    String getRowKey(ResultSet rs) throws SQLException;
}
