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
package com.treasuredata.jdbc.model;

import com.treasuredata.jdbc.TDTypeConverter;

import java.sql.SQLException;
import java.sql.Types;

public class TDColumn
{
    private final String columnName;

    private final String tableName;

    private final String tableCatalog;

    private final String type;

    private final String comment;

    private final int ordinal;

    public TDColumn(String columnName, String tableName, String tableCatalog,
            String type, String comment, int ordinal)
    {
        this.columnName = columnName;
        this.tableName = tableName;
        this.tableCatalog = tableCatalog;
        this.type = type;
        this.comment = comment;
        this.ordinal = ordinal;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getTableCatalog()
    {
        return tableCatalog;
    }

    public String getType()
    {
        return type;
    }

    public Integer getSqlType()
            throws SQLException
    {
        return TDTypeConverter.TDTypeToSqlType(type);
    }

    public static int columnDisplaySize(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
                return columnPrecision(columnType);
            case Types.VARCHAR:
                return Integer.MAX_VALUE; // hive has no max limit for strings
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return columnPrecision(columnType) + 1; // allow +/-

            // see
            // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
            case Types.FLOAT:
                return 24; // e.g. -(17#).e-###
            // see
            // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
            case Types.DOUBLE:
                return 25; // e.g. -(17#).e-####
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public static int columnPrecision(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
                return 1;
            case Types.VARCHAR:
                return Integer.MAX_VALUE; // hive has no max limit for strings
            case Types.TINYINT:
                return 3;
            case Types.SMALLINT:
                return 5;
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.FLOAT:
                return 7;
            case Types.DOUBLE:
                return 15;
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public static int columnScale(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
            case Types.VARCHAR:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return 0;
            case Types.FLOAT:
                return 7;
            case Types.DOUBLE:
                return 15;
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public Integer getColumnSize()
            throws SQLException
    {
        int precision = columnPrecision(TDTypeConverter.TDTypeToSqlType(type));
        return precision == 0 ? null : precision;
    }

    public Integer getDecimalDigits()
            throws SQLException
    {
        return columnScale(TDTypeConverter.TDTypeToSqlType(type));
    }

    public Integer getNumPrecRadix()
    {
        if (type.equalsIgnoreCase("tinyint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("smallint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("int")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("bigint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("float")) {
            return 2;
        }
        else if (type.equalsIgnoreCase("double")) {
            return 2;
        }
        else { // anything else including boolean and string is null
            return null;
        }
    }

    public String getComment()
    {
        return comment;
    }

    public int getOrdinal()
    {
        return ordinal;
    }
}
