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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 *
 */
public class TDStatement implements Statement
{
    private static final int NO_LIMIT = 0;

    private int queryTimeOutSeconds = NO_LIMIT;
    private int maxRows = NO_LIMIT;


    @Override
    public ResultSet executeQuery(String s)
            throws SQLException
    {
        return null;
    }

    @Override
    public int executeUpdate(String s)
            throws SQLException
    {
        return 0;
    }

    @Override
    public void close()
            throws SQLException
    {

    }

    @Override
    public int getMaxFieldSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i)
            throws SQLException
    {

    }

    @Override
    public int getMaxRows()
            throws SQLException
    {
        return maxRows;
    }

    @Override
    public void setMaxRows(int maxRows)
            throws SQLException
    {
        if(maxRows < 0) {
            throw new SQLException("maxRows must be >= 0");
        }
        this.maxRows = maxRows;
    }

    @Override
    public void setEscapeProcessing(boolean b)
            throws SQLException
    {

    }

    @Override
    public int getQueryTimeout()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setQueryTimeout(int timeout)
            throws SQLException
    {

    }

    @Override
    public void cancel()
            throws SQLException
    {

    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {

    }

    @Override
    public void setCursorName(String s)
            throws SQLException
    {

    }

    @Override
    public boolean execute(String s)
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        return null;
    }

    @Override
    public int getUpdateCount()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean getMoreResults()
            throws SQLException
    {
        return false;
    }

    @Override
    public void setFetchDirection(int i)
            throws SQLException
    {

    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setFetchSize(int i)
            throws SQLException
    {

    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getResultSetConcurrency()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getResultSetType()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void addBatch(String s)
            throws SQLException
    {

    }

    @Override
    public void clearBatch()
            throws SQLException
    {

    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        return new int[0];
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean getMoreResults(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys()
            throws SQLException
    {
        return null;
    }

    @Override
    public int executeUpdate(String s, int i)
            throws SQLException
    {
        return 0;
    }

    @Override
    public int executeUpdate(String s, int[] ints)
            throws SQLException
    {
        return 0;
    }

    @Override
    public int executeUpdate(String s, String[] strings)
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean execute(String s, int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean execute(String s, int[] ints)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean execute(String s, String[] strings)
            throws SQLException
    {
        return false;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    @Override
    public void setPoolable(boolean b)
            throws SQLException
    {

    }

    @Override
    public boolean isPoolable()
            throws SQLException
    {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass)
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass)
            throws SQLException
    {
        return false;
    }
}
