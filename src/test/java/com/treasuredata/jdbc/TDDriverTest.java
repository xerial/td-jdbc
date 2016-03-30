/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.jdbc;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Driver;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TDDriverTest
{
    @Test
    public void versionCheck()
            throws Exception
    {
        // Prepare version file in the class path
        File pomPropFile = new File("target/test-classes", TDDriver.pomProperties.substring(1));
        Properties p = new Properties();
        p.setProperty("version", "2.10.1");
        p.store(new FileWriter(pomPropFile), "");

        Driver driver = new TDDriver();
        assertEquals(2, driver.getMajorVersion());
        assertEquals(10, driver.getMinorVersion());
    }
}