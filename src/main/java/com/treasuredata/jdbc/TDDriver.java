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

import com.google.common.collect.ImmutableList;
import com.treasuredata.client.TDClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 */
public class TDDriver
        implements Driver
{
    private static final Logger logger = LoggerFactory.getLogger(TDDriver.class);
    private static int majorVersion = 1;
    private static int minorVersion = 0;

    static {
        // Acquir major/minor version from Maven pom.properties
        URL mavenProperties = TDDriver.class.getResource("/META-INF/maven/com.treasuredata.jdbc/td-jdbc/pom.properties");
        Option<String> version = readMavenVersion(mavenProperties);
        if (version.isDefined()) {
            String v = version.get();
            logger.info("td-jdbc version: " + v);
            String[] c = v.split("\\.");
            if (c.length > 0) {
                majorVersion = safeParseInt(c[0], majorVersion);
            }
            if (c.length > 1) {
                minorVersion = safeParseInt(c[1], minorVersion);
            }
        }
    }

    private static int safeParseInt(String s, int defaultValue)
    {
        try {
            return Integer.parseInt(s);
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static Option<String> readMavenVersion(URL mavenProperties)
    {
        if (mavenProperties != null) {
            InputStream in = null;
            try {
                in = mavenProperties.openStream();
                Properties p = new Properties();
                p.load(in);
                return Option.of(p.getProperty("version"));
            }
            catch (Throwable e) {
                logger.warn("Error in reading pom.properties file", e);
            }
            finally {
                if (in != null) {
                    try {
                        in.close();
                    }
                    catch (IOException e) {
                        logger.error("Failed to close resource", e);
                    }
                }
            }
        }
        return Option.empty();
    }

    @Override
    public Connection connect(String jdbcUrl, Properties properties)
            throws SQLException
    {
        if(jdbcUrl == null) {
            throw new SQLException("jdbcUrl is null");
        }
        if(properties == null)  {
            throw new SQLException("jdbc properties are null");
        }
        Config config = Config.parseJdbcURL(jdbcUrl).setProperties(properties);
        return new TDConnection(config);
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        if (url == null) {
            throw new SQLException("Url is null");
        }

        try {
            Config.parseJdbcURL(url);
            return true;
        }
        catch (SQLException e) {
            logger.debug("Invalid url: " + url, e);
            return false;
        }
    }

    private static class DriverPropertyBuilder
    {
        private final ImmutableList.Builder<DriverPropertyInfo> builder = ImmutableList.builder();
        private final Properties givenProps;

        public DriverPropertyBuilder(Properties props)
        {
            givenProps = props;
        }

        public void add(String propName, String description, boolean isRequired)
        {
            DriverPropertyInfo pi = new DriverPropertyInfo("user", givenProps.getProperty("user"));
            pi.description = description;
            pi.required = isRequired;
            builder.add(pi);
        }

        public DriverPropertyInfo[] build()
        {
            return builder.build().toArray(new DriverPropertyInfo[0]);
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String jdbcUrl, Properties properties)
            throws SQLException
    {
        Config config = Config.newConfig(jdbcUrl, properties);

        DriverPropertyBuilder builder = new DriverPropertyBuilder(config.toProperties());
        builder.add("apikey", "API key to access Treasure Data", false);
        builder.add("user", "User's account e-mail address", false);
        builder.add("password", "User's account password", false);
        builder.add("type", "Query engine type. Default is presto", false);
        builder.add("useSSL", "Use ssl encription", false);
        builder.add("host", "Host name of TD API", false);
        builder.add("httpproxyhost", "Proxy host", false);
        builder.add("httpproxyport", "Proxy port", false);
        builder.add("httpproxyuser", "Proxy user", false);
        builder.add("httpproxypassword", "Proxy password", false);

        // Add TDClient's configuration parameters
        for(String p : TDClientConfig.knownProperties) {
           builder.add(p, p, false);
        }

        return builder.build();
    }

    @Override
    public int getMajorVersion()
    {
        return majorVersion;
    }

    @Override
    public int getMinorVersion()
    {
        return minorVersion;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // To make it true, we need to support SQL-92 standards. Note MySQL, PostgreSQL JDBC drivers also return false here.
        return false;
    }
}
