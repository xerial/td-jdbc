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

import com.treasuredata.client.model.TDJob;
import com.treasuredata.jdbc.command.CommandContext;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static com.treasuredata.jdbc.TDJDBCException.UNSUPPORTED;

public class TDPreparedStatement
        extends OldTDStatement
        implements PreparedStatement
{
    private static Logger LOG = Logger.getLogger(
            TDPreparedStatement.class.getName());

    private CommandContext context;
    private Map<Integer, String> preparedParameters = new HashMap<Integer, String>();

    public TDPreparedStatement(TDConnection conn, String sql)
            throws SQLException
    {
        super(conn);
        context = createCommandContext(sql);
    }

    CommandContext getContext()
    {
        return context;
    }

    Map<Integer, String> getParams()
    {
        return preparedParameters;
    }

    public void clearParameters()
            throws SQLException
    {
        preparedParameters.clear();
    }

    public void addBatch()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void clearBatch()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean execute()
            throws SQLException
    {
        return executeQuery() != null;
    }

    public synchronized ResultSet executeQuery()
            throws SQLException
    {
        String sql = context.sql;
        if (sql.contains("?")) {
            sql = updateSql(sql, preparedParameters);
        }
        return executeQuery(sql);
    }

    String updateSql(final String sql, Map<Integer, String> parameters)
    {
        StringBuffer newSql = new StringBuffer(sql);

        int paramLoc = 1;
        while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
            // check the user has set the needs parameters
            if (parameters.containsKey(paramLoc)) {
                int tt = getCharIndexFromSqlByParamLocation(newSql.toString(), '?', 1);
                newSql.deleteCharAt(tt);
                newSql.insert(tt, parameters.get(paramLoc));
            }
            paramLoc++;
        }

        return newSql.toString();
    }

    private int getCharIndexFromSqlByParamLocation(final String sql,
            final char cchar, final int paramLoc)
    {
        boolean escaping = false;
        boolean quoted = false;
        int num = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (escaping) {
                escaping = false;
                continue;
            }
            char c = sql.charAt(i);
            switch (c) {
                case '\\':
                    escaping = true;
                    break;
                case '\'':
                    // record the count of char "'"
                    quoted = !quoted;
                    break;
                default:
                    if (c == cchar && !quoted) {
                        num++;
                        if (num == paramLoc) {
                            return i;
                        }
                    }
                    break;
            }
        }
        return -1;
    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int executeUpdate()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return null;
    }

    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setArray(int i, Array x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setAsciiStream(int i, InputStream in)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setAsciiStream(int i, InputStream in, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setAsciiStream(int i, InputStream in, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBinaryStream(int i, InputStream x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBinaryStream(int i, InputStream in, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBinaryStream(int i, InputStream in, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBlob(int i, Blob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBlob(int i, InputStream inputStream)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBlob(int i, InputStream inputStream, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setByte(int i, byte x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setBytes(int i, byte[] x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setCharacterStream(int i, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setCharacterStream(int i, Reader reader, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setCharacterStream(int i, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setClob(int i, Clob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setClob(int i, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setClob(int i, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setDate(int i, Date x)
            throws SQLException
    {
        if (conn.getConfig().type == TDJob.Type.PRESTO) {
            preparedParameters.put(i, "DATE '" + x.toString() + "'");
        }
        else {
            throw new SQLException(new UnsupportedOperationException(
                    "TDPreparedStatement#setDate(int, Date) is supported only for Presto query"));
        }
    }

    public void setDate(int i, Date x, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setDouble(int i, double x)
            throws SQLException
    {
        preparedParameters.put(i, Double.toString(x));
    }

    public void setFloat(int i, float x)
            throws SQLException
    {
        preparedParameters.put(i, Float.toString(x));
    }

    public void setInt(int i, int x)
            throws SQLException
    {
        preparedParameters.put(i, Integer.toString(x));
    }

    public void setLong(int i, long x)
            throws SQLException
    {
        preparedParameters.put(i, Long.toString(x));
    }

    public void setNCharacterStream(int i, Reader value)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNCharacterStream(int i, Reader value, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNClob(int i, NClob value)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNClob(int i, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNClob(int i, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNString(int i, String value)
            throws SQLException
    {
        setString(i, value);
    }

    public void setNull(int i, int sqlType)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setNull(int i, int sqlType, String typeName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setObject(int i, Object x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setObject(int i, Object x, int targetSqlType)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setObject(int i, Object x, int targetSqlType, int scale)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setRef(int i, Ref x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setRowId(int i, RowId x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setSQLXML(int i, SQLXML xmlObject)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setShort(int i, short x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setString(int i, String x)
            throws SQLException
    {
        x = x.replace("'", "\\'");
        preparedParameters.put(i, "'" + x + "'");
    }

    public void setTime(int i, Time x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setTime(int i, Time x, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setTimestamp(int i, Timestamp x)
            throws SQLException
    {
        if (conn.getConfig().type == TDJob.Type.PRESTO) {
            preparedParameters.put(i, "TIMESTAMP '" + x.toString() + "'");
        }
        else {
            throw new SQLException(new UnsupportedOperationException(
                    "TDPreparedStatement#setTimestamp(int, Timestamp) is supported only for Presto query"));
        }
    }

    public void setTimestamp(int i, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setURL(int i, URL x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setUnicodeStream(int i, InputStream x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }
}
