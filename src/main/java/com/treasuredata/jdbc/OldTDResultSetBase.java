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

import org.msgpack.core.MessageTypeException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static com.treasuredata.jdbc.TDJDBCException.UNSUPPORTED;

/**
 * Data independed base class which implements the common part of all
 * resultsets.
 */
public abstract class OldTDResultSetBase
        implements ResultSet
{
    private static final String INTVALUE_CLASSNAME = "org.msgpack.type.IntValueImpl";
    private static final String LONGVALUE_CLASSNAME = "org.msgpack.type.LongValueImpl";
    private static final String DOUBLEVALUE_CLASSNAME = "org.msgpack.type.DoubleValueImpl";
    private static final String FLOATVALUE_CLASSNAME = "org.msgpack.type.FloatValueImpl";

    protected SQLWarning warningChain = null;

    protected boolean wasNull = false;

    protected ArrayValue row;

    protected List<String> columnNames;

    protected List<String> columnTypes;

    protected OldTDStatementBase statement = null;

    public boolean absolute(int row)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void afterLast()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void beforeFirst()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void cancelRowUpdates()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void deleteRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int findColumn(String columnName)
            throws SQLException
    {
        int columnIndex = columnNames.indexOf(columnName);
        if (columnIndex == -1) {
            throw new SQLException("columnIndex: -1");
        }
        else {
            return ++columnIndex;
        }
    }

    public boolean first()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getAsciiStream(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getBinaryStream(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Reader getCharacterStream(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Reader getNCharacterStream(int index)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Reader getNCharacterStream(String name)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Array getArray(int i)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Array getArray(String colName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public BigDecimal getBigDecimal(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Blob getBlob(int i)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Blob getBlob(String colName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean getBoolean(int index)
            throws SQLException
    {
        return this.getBooleanWithTypeConversion(index);
    }

    public boolean getBoolean(String name)
            throws SQLException
    {
        return getBoolean(findColumn(name));
    }

    private boolean getBooleanWithTypeConversion(int index)
            throws SQLException
    {
        return TDTypeConverter.convertToBoolean(getValue(index));
    }

    public byte getByte(int index)
            throws SQLException
    {
        return getByteWithImplicitTypeConversion(index);
    }

    public byte getByte(String name)
            throws SQLException
    {
        return getByte(findColumn(name));
    }

    private byte getByteWithImplicitTypeConversion(int index)
            throws SQLException
    {
        return (byte) TDTypeConverter.convertToInt(getValue(index));
    }

    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public byte[] getBytes(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Clob getClob(int i)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Clob getClob(String colName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public NClob getNClob(int index)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public String getNString(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public String getNString(String columnLabel)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int getConcurrency()
            throws SQLException
    {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public String getCursorName()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Date getDate(int index)
            throws SQLException
    {
        return TDTypeConverter.convertToDate(getValue(index));
    }

    public Date getDate(String columnName)
            throws SQLException
    {
        return getDate(findColumn(columnName));
    }

    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Date getDate(String columnName, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public double getDouble(int index)
            throws SQLException
    {
        return getDoubleWithImplicitTypeConversion(index);
    }

    public double getDouble(String name)
            throws SQLException
    {
        return getDouble(findColumn(name));
    }

    private double getDoubleWithImplicitTypeConversion(int index)
            throws SQLException
    {
        return TDTypeConverter.convertToDouble(getValue(index));
    }

    public int getFetchDirection()
            throws SQLException
    {
        return ResultSet.FETCH_FORWARD;
    }

    public int getFetchSize()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public float getFloat(int index)
            throws SQLException
    {
        return getFloatWithImplicitTypeConversion(index);
    }

    public float getFloat(String name)
            throws SQLException
    {
        return getFloat(findColumn(name));
    }

    private float getFloatWithImplicitTypeConversion(int index)
            throws SQLException
    {
        return TDTypeConverter.convertToFloat(getValue(index));
    }

    public int getHoldability()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int getInt(int index)
            throws SQLException
    {
        return getIntWithImplicitTypeConversion(index);
    }

    public int getInt(String name)
            throws SQLException
    {
        return getInt(findColumn(name));
    }

    protected Value getValue(int index)
    {
        return row.get(index);
    }

    private int getIntWithImplicitTypeConversion(int index)
            throws SQLException
    {
        try {
            return TDTypeConverter.convertToInt(getValue(index));
        }
        catch (MessageTypeException e) {
            throw new SQLException(e);
        }
    }

    public long getLong(int index)
            throws SQLException
    {
        return getLongWithImplicitTypeConversion(index);
    }

    public long getLong(String name)
            throws SQLException
    {
        return getLong(findColumn(name));
    }

    private long getLongWithImplicitTypeConversion(int index)
            throws SQLException
    {
        try {
            return TDTypeConverter.convertToLong(getValue(index));
        }
        catch (MessageTypeException e) {
            throw new SQLException(e);
        }
    }

    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return new TDResultSetMetaData(columnNames, columnTypes);
    }

    public Object getObject(int columnIndex)
            throws SQLException
    {
        if (row == null) {
            throw new SQLException("No row found. If you don't call ResultSet#next method, please call it before calling getObject method to fetch rows.");
        }

        if (columnIndex > row.size()) {
            throw new SQLException("Invalid columnIndex: " + columnIndex);
        }

        try {
            wasNull = false;
            if (row.get(columnIndex - 1) == null) {
                wasNull = true;
            }

            return row.get(columnIndex - 1);
        }
        catch (Exception e) {
            throw new SQLException(e.toString());
        }
    }

    public Object getObject(String columnName)
            throws SQLException
    {
        return getObject(findColumn(columnName));
    }

    public Object getObject(int i, Map<String, Class<?>> map)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Object getObject(String colName, Map<String, Class<?>> map)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Ref getRef(int i)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Ref getRef(String colName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int getRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public short getShort(int index)
            throws SQLException
    {
        return getShortWithImplicitTypeConversion(index);
    }

    public short getShort(String name)
            throws SQLException
    {
        return getShort(findColumn(name));
    }

    private short getShortWithImplicitTypeConversion(int index)
            throws SQLException
    {
        return (short) TDTypeConverter.convertToInt(getValue(index));
    }

    void setStatement(OldTDStatementBase stat)
    {
        statement = stat;
    }

    public Statement getStatement()
            throws SQLException
    {
        return statement;
    }

    /**
     * @param index - the first column is 1, the second is 2, ...
     * @see java.sql.ResultSet#getString(int)
     */
    public String getString(int index)
            throws SQLException
    {
        return getStringWithImplicitTypeConversion(index);
    }

    public String getString(String name)
            throws SQLException
    {
        return getString(findColumn(name));
    }

    public String getStringWithImplicitTypeConversion(int index)
            throws SQLException
    {
        return getValue(index).toString();
    }

    public Time getTime(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Time getTime(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Time getTime(String columnName, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Timestamp getTimestamp(int index)
            throws SQLException
    {
        return TDTypeConverter.convertToTimestamp(getValue(index));
    }

    public Timestamp getTimestamp(String name)
            throws SQLException
    {
        return getTimestamp(findColumn(name));
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public int getType()
            throws SQLException
    {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public URL getURL(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public URL getURL(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public InputStream getUnicodeStream(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void insertRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean isAfterLast()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean isLast()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean last()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean isBeforeFirst()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean isFirst()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    public void moveToCurrentRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void moveToInsertRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean previous()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void refreshRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean relative(int rows)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean rowDeleted()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean rowInserted()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public boolean rowUpdated()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setFetchDirection(int direction)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void setFetchSize(int rows)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateArray(String columnName, Array x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(String columnName, InputStream x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(String columnName, InputStream x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBinaryStream(String columnLabel, InputStream x,
            long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(String columnName, Blob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBlob(String columnLabel, InputStream inputStream,
            long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBoolean(String columnName, boolean x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateByte(String columnName, byte x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateBytes(String columnName, byte[] x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(String columnName, Reader reader,
            int length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateCharacterStream(String columnLabel, Reader reader,
            long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(String columnName, Clob x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateDate(String columnName, Date x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateDouble(String columnName, double x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateFloat(String columnName, float x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateInt(String columnName, int x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateLong(String columnName, long x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader,
            long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(int columnIndex, NClob clob)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(String columnLabel, NClob clob)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNString(int columnIndex, String string)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNString(String columnLabel, String string)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNull(int columnIndex)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateNull(String columnName)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateObject(String columnName, Object x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateObject(String columnName, Object x, int scale)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateRef(String columnName, Ref x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateRow()
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateShort(String columnName, short x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateString(String columnName, String x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateTime(String columnName, Time x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public SQLWarning getWarnings()
            throws SQLException
    {
        return warningChain;
    }

    public void clearWarnings()
            throws SQLException
    {
        warningChain = null;
    }

    public boolean wasNull()
            throws SQLException
    {
        return wasNull;
    }

    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        throw UNSUPPORTED();
    }

    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        throw UNSUPPORTED();
    }
}
