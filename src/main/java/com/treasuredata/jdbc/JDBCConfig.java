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


import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClientBuilder;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.model.TDJob;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC connection configuration
 */
public class JDBCConfig
{
    public static final String TD_JDBC_USESSL = "usessl";
    public static final String TD_JDBC_USER = "user";
    public static final String TD_JDBC_PASSWORD = "password";
    public static final String TD_JDBC_APIKEY = "apikey";
    public static final String TD_JDBC_JOB_TYPE = "type";
    public static final String TD_JDBC_PROXY_HOST = "httpproxyhost";
    public static final String TD_JDBC_PROXY_PORT = "httpproxyport";
    public static final String TD_JDBC_PROXY_USER = "httpproxyuser";
    public static final String TD_JDBC_PROXY_PASSWORD = "httpproxypassword";

    // Mapping from jdbc properties to td-client ones
    static Map<String, String> jdbcToClientConfigMapping;
    static {
        ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
        b.put(TD_JDBC_USESSL, TDClientConfig.TD_CLIENT_USESSL);
        b.put(TD_JDBC_USER, TDClientConfig.TD_CLIENT_USER);
        b.put(TD_JDBC_PASSWORD, TDClientConfig.TD_CLIENT_PASSOWRD);
        b.put(TD_JDBC_APIKEY, TDClientConfig.TD_CLIENT_APIKEY);
        b.put(TD_JDBC_PROXY_HOST, TDClientConfig.TD_CLIENT_PROXY_HOST);
        b.put(TD_JDBC_PROXY_PORT, TDClientConfig.TD_CLIENT_PROXY_PORT);
        b.put(TD_JDBC_PROXY_USER, TDClientConfig.TD_CLIENT_PROXY_USER);
        b.put(TD_JDBC_PROXY_PASSWORD, TDClientConfig.TD_CLIENT_PROXY_PASSWORD);
        jdbcToClientConfigMapping = b.build();
    }

    // TD JDBC Configuration
    public final String url;
    public final String database;
    public final TDJob.Type type;
    public final TDClientConfig clientConfig;

    public JDBCConfig(
            String url,
            String database,
            TDJob.Type type,
            TDClientConfig clientConfig
    )
            throws SQLException
    {
        this.url = url;
        this.database = database;
        if (type == null || !(type.equals(TDJob.Type.HIVE) || type.equals(TDJob.Type.PRESTO))) {
            throw new SQLException("invalid job type within URL: " + type);
        }
        this.type = type;
        this.clientConfig = clientConfig;
    }

    public Properties toProperties() {
        Properties prop = new Properties();
        prop.setProperty(TD_JDBC_JOB_TYPE, type.getType());
        prop.putAll(clientConfig.toProperties());
        return prop;
    }

    public static void validateJDBCUrl(String url)
            throws SQLException
    {
        if (url == null || url.isEmpty() || !url.startsWith(TDDriver.URL_PREFIX)) {
            throw new SQLException("Invalid JDBC URL: " + url + ". URL prefix must be jdbc:td://");
        }
    }

    private static String getJDBCProperty(Properties prop, String key) {
        return System.getProperty(key, prop.getProperty(key));
    }

    private static String getJDBCProperty(Properties prop, String key, String anotherKey) {
        return System.getProperty(key, System.getProperty(anotherKey, prop.getProperty(key, prop.getProperty(anotherKey))));
    }

    /**
     * Apply current configuration to System properties if necessary
     */
    public void apply() {
        if(apiConfig.proxy.isDefined()) {
            apiConfig.proxy.get().apply();
        }
    }

    /**
     * Create a new connection configuration from a given JDBC URL and custom Properties.
     * The precedence of properties is:
     *
     * <p>
     *  <li> 1. System properties </li>
     *  <li> 2. Properties object </li>
     *  <li> 3. URL parameters </li>
     * </p>
     * @param jdbcUrl jdbc url
     * @param props jdbc properties
     * @return connection configuration
     * @throws SQLException
     */
    public static JDBCConfig newConfig(String jdbcUrl, Properties props)
            throws SQLException {
        return parseJdbcURL(jdbcUrl).setProperties(props);
    }

    /**
     *  jdbc:td://api.treasure-data.com:80/testdb;k1=v1;k2=v2
     *  +-------+ +-------------------+ ++ +----+ +---------+
     *  type      host               port  path   parameters
     *  +----------------------+
     *  endpoint
     *
     */
    public static JDBCConfig parseJdbcURL(String jdbcUrl)
            throws SQLException
    {
        validateJDBCUrl(jdbcUrl);

        ConfigBuilder config = new ConfigBuilder();
        config.setUrl(jdbcUrl);

        TDClientBuilder clientConfig = new TDClientBuilder(false);

        int postUrlPos = jdbcUrl.length();
        // postUrlPos is the END position in url String,
        // wrt what remains to be processed.
        // i.e., if postUrlPos is 100, url no longer needs to examined at
        // index 100 or later.

        int semiPos = jdbcUrl.indexOf(';', TDDriver.URL_PREFIX.length());
        if (semiPos < 0) {
            semiPos = postUrlPos;
        }
        else {
            String params = jdbcUrl.substring(semiPos + 1, postUrlPos);
            String[] kvs = params.split(";");
            for (String param : kvs) {
                String[] kv = param.split("=");
                if (kv == null || kv.length != 2) {
                    throw new SQLException("invalid parameters within URL: " + jdbcUrl);
                }

                String k = kv[0].toLowerCase();
                String v = kv[1];
                if (k.equals(TD_JDBC_USER)) {
                    config.setUser(v);
                }
                else if (k.equals(TD_JDBC_PASSWORD)) {
                    config.setPassword(v);
                }
                else if (k.equals(TD_JDBC_JOB_TYPE)) {
                    config.setType(TDJob.Type.fromString(v));
                }
                else if (k.equals(TD_JDBC_USESSL)) {
                    apiConfig.setUseSSL(Boolean.parseBoolean(kv[1].toLowerCase()));
                }
                else if (k.equals(TD_JDBC_PROXY_HOST)) {
                    hasProxyConfig = true;
                    proxyConfig.setHost(v);
                }
                else if (k.equals(TD_JDBC_PROXY_PORT)) {
                    hasProxyConfig = true;
                    proxyConfig.setPort(Integer.parseInt(v));
                }
                else if (k.equals(TD_JDBC_PROXY_USER)) {
                    hasProxyConfig = true;
                    proxyConfig.setUser(v);
                }
                else if (k.equals(TD_JDBC_PROXY_PASSWORD)) {
                    hasProxyConfig = true;
                    proxyConfig.setPassword(v);
                }
            }
        }

        int slashPos = jdbcUrl.indexOf('/', URL_PREFIX.length());
        if(slashPos == -1) {
            slashPos = jdbcUrl.indexOf(';', URL_PREFIX.length());
        }


        // Set the API endpoint specified in JDBC URL
        String endpoint = null;
        try {
            endpoint = jdbcUrl.substring(URL_PREFIX.length(), slashPos);
        }
        catch (IndexOutOfBoundsException t) {
            throw new SQLException("invalid endpoint within URL: " + jdbcUrl);
        }
        if (endpoint != null && !endpoint.isEmpty()) {
            int i = endpoint.indexOf(':');
            if (i >= 0) {
                if (i != 0) {
                    apiConfig.setEndpoint(endpoint.substring(0, i));
                }

                String portStr = endpoint.substring(i + 1, endpoint.length());
                try {
                    apiConfig.setPort(Integer.parseInt(portStr));
                }
                catch (NumberFormatException t) {
                    throw new SQLException(String.format("invalid port '%s' within URL: %s", portStr, jdbcUrl));
                }
            }
            else { // i < 0
                apiConfig.setEndpoint(endpoint);
            }
        }

        // Set database name
        if (slashPos >= 0 && jdbcUrl.charAt(slashPos) == '/') {
            try {
                String rawDatabase = jdbcUrl.substring(slashPos + 1, semiPos);
                if (rawDatabase != null && !rawDatabase.isEmpty()) {
                    config.setDatabase(rawDatabase);
                }
            }
            catch (IndexOutOfBoundsException t) {
                throw new SQLException("invalid database name within URL: " + jdbcUrl);
            }
        }

        if (hasProxyConfig) {
            apiConfig.setProxyConfig(proxyConfig.createProxyConfig());
        }
        config.setApiConfig(apiConfig.createApiConfig());
        return config.createConnectionConfig();
    }

    private static boolean isEmptyString(String s) {
        return s == null || (s != null && s.isEmpty());
    }

    /**
     * Overwrite the configuration with given Properties then System properties.
     *
     * @param props
     * @return
     * @throws SQLException
     */
    public JDBCConfig setProperties(Properties props) throws SQLException {

        ConfigBuilder config = new ConfigBuilder(this);
        ApiConfig.ApiConfigBuilder apiConfig = new ApiConfig.ApiConfigBuilder(this.apiConfig);
        ProxyConfig.ProxyConfigBuilder proxyConfig;
        boolean hasProxyConfig = false;
        if(apiConfig.proxy.isDefined()) {
            proxyConfig = new ProxyConfig.ProxyConfigBuilder(apiConfig.proxy.get());
            hasProxyConfig = true;
        }
        else {
            proxyConfig = new ProxyConfig.ProxyConfigBuilder();
        }

        // Override URL parameters via Properties, then System properties.

        // api endpoint
        String apiHost = getJDBCProperty(props, TDClientConfig.TD_CLIENT_API_ENDPOINT);
        if(apiHost != null) {
            apiConfig.setEndpoint(apiHost);
        }
        // api port
        String apiPortStr = getJDBCProperty(props, TDClientConfig.TD_CLIENT_API_PORT);
        if(apiPortStr != null) {
            try {
                apiConfig.setPort(Integer.parseInt(apiPortStr));
            }
            catch (NumberFormatException e) {
                throw new SQLException("port number is invalid: " + apiPortStr);
            }
        }

        // API scheme (HTTP or HTTPS)
        String useSSL = getJDBCProperty(props, TD_JDBC_USESSL);
        if (useSSL != null) {
            apiConfig.setUseSSL(Boolean.parseBoolean(useSSL));
        }

        // TD API key
        String apiKey = null;
        // Check environment variable
        if(System.getenv().containsKey("TD_API_KEY")) {
            apiKey = System.getenv().get("TD_API_KEY");
        }
        if(apiKey == null) {
            apiKey = getJDBCProperty(props, TD_JDBC_APIKEY, TDClientConfig.TD_CLIENT_APIKEY);
        }

        if(isEmptyString(apiKey)) {
            apiKey = null;
        }
        else {
            apiConfig.setApiKey(apiKey);
        }

        // user
        String user = getJDBCProperty(props, TD_JDBC_USER);
        if (apiKey == null && isEmptyString(user)) {
            throw new SQLException("User is not specified. Use Properties object to set 'user'");
        }
        config.setUser(user);

        // password
        String password = getJDBCProperty(props, TD_JDBC_PASSWORD);
        if (apiKey == null && isEmptyString(password)) {
            throw new SQLException("Password is not specified. Use Properties object to set 'password'");
        }
        config.setPassword(password);

        // If both user and password are specified, use this pair instead of TD_API_KEY
        if(!isEmptyString(user) && !isEmptyString(password)) {
            apiConfig.unsetApiKey();
        }

//        // retry settings
//        String retryCountThreshold = getJDBCProperty(props, TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD);
//        if(retryCountThreshold != null) {
//            try {
//                config.setResultRetryCountThreshold(Integer.parseInt(retryCountThreshold));
//            }
//            catch (NumberFormatException e) {
//                throw new SQLException("Invalid value for td.jdbc.result.retrycount.threshold: " + retryCountThreshold);
//            }
//        }
//        String retryWaitTimeMs = getJDBCProperty(props, TD_JDBC_RESULT_RETRY_WAITTIME);
//        if(retryWaitTimeMs != null) {
//            try {
//                config.setResultRetryWaitTimeMs(Long.parseLong(retryWaitTimeMs));
//            }
//            catch (NumberFormatException e) {
//                throw new SQLException("Invalid value for td.jdbc.result.retry.waittime: " + retryWaitTimeMs);
//            }
//        }

        // proxy settings: host and port are supported by Java runtime.
        // http.proxyUser and http.proxyPassword are no longer supported but
        // we set Authenticator by ourselves.

        String httpProxyHost = getJDBCProperty(props, TD_JDBC_PROXY_HOST, "http.proxyHost");
        if (httpProxyHost != null) {
            hasProxyConfig = true;
            proxyConfig.setHost(httpProxyHost);
        }
        String httpProxyPort = getJDBCProperty(props, TD_JDBC_PROXY_PORT, "http.proxyPort");
        if (httpProxyPort != null) {
            hasProxyConfig = true;
            try {
                proxyConfig.setPort(Integer.parseInt(httpProxyPort));
            }
            catch(NumberFormatException e) {
                throw new SQLException("Proxy port is not a number: " + httpProxyPort);
            }
        }
        String httpProxyUser = getJDBCProperty(props, TD_JDBC_PROXY_USER, "http.proxyUser");
        if (httpProxyUser != null) {
            hasProxyConfig = true;
            proxyConfig.setUser(httpProxyUser);
        }
        String httpProxyPassword = getJDBCProperty(props, TD_JDBC_PROXY_PASSWORD, "http.proxyPassword");
        if (httpProxyPassword != null) {
            hasProxyConfig = true;
            proxyConfig.setPassword(httpProxyPassword);
        }

        if (hasProxyConfig) {
            apiConfig.setProxyConfig(proxyConfig.createProxyConfig());
        }
        config.setApiConfig(apiConfig.createApiConfig());
        return config.createConnectionConfig();
    }

}
