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

public enum TDDataType
{
    LONG_VARCHAR("LONG VARCHAR", -1, 32700, ",", ",", null, (short) 1, true, (short) 1,
            true, false, false, "LONG VARCHAR", null, null, null, null, null),
    VARCHAR("VARCHAR", 12, 32672, ",", ",", "length", (short) 1, true, (short) 3,
            true, false, false, "VARCHAR", null, null, null, null, null),
    CHAR("CHAR", 1, 254, ",", ",", "length", (short) 1, true, (short) 3,
            true, false, false, "CHAR", null, null, null, null, null),
    BIGINT("BIGINT", -5, 19, null, null, null, (short) 1, false, (short) 2,
            false, false, true, "BIGINT", (short) 0, (short) 0, null, null, 0),
    INTEGER("INTEGER", 4, 10, null, null, null, (short) 1, false, (short) 2,
            false, false, true, "INTEGER", (short) 0, (short) 0, null, null, 0),
    SMALLINT("SMALLINT", 5, 5, null, null, null, (short) 1, false, (short) 2,
            false, false, true, "SMALLINT", (short) 0, (short) 0, null, null, 0),
    FLOAT("FLOAT", 6, 52, null, null, "precision", (short) 1, false, (short) 2,
            false, false, false, "FLOAT", null, null, null, null, 2),
    DOUBLE("DOUBLE", 8, 52, null, null, null, (short) 1, false, (short) 2,
            false, false, false, "DOUBLE", null, null, null, null, 2);

    private final String typeName;
    private final Integer dataType;
    private final Integer precision;
    private final String literalPrefix;
    private final String literalSuffix;
    private final String createParams;
    private final Short nullable;
    private final Boolean caseSensitive;
    private final Short searchable;
    private final Boolean unsignedAttr;
    private final Boolean fixedPrecScale;
    private final Boolean autoIncrement;
    private final String localTypeName;
    private final Short minimumScale;
    private final Short maximumScale;
    private final Integer sqlDataType;
    private final Integer sqlDatetimeSub;
    private final Integer numPrecRadix;

    private TDDataType(String name, Integer dataType, Integer precision,
            String literalPrefix, String literalSuffix, String createParams,
            Short nullable, Boolean caseSensitive, Short searchable,
            Boolean unsignedAttr, Boolean fixedPrecScale, Boolean autoIncrement,
            String localTypeName, Short minimunScale, Short maximumScale,
            Integer sqlDataType, Integer sqlDatetimeSub, Integer numPrecRadix)
    {
        this.typeName = name;
        this.dataType = dataType;
        this.precision = precision;
        this.literalPrefix = literalPrefix;
        this.literalSuffix = literalSuffix;
        this.createParams = createParams;
        this.nullable = nullable;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.unsignedAttr = unsignedAttr;
        this.fixedPrecScale = fixedPrecScale;
        this.autoIncrement = autoIncrement;
        this.localTypeName = localTypeName;
        this.minimumScale = minimunScale;
        this.maximumScale = maximumScale;
        this.sqlDataType = sqlDataType;
        this.sqlDatetimeSub = sqlDatetimeSub;
        this.numPrecRadix = numPrecRadix;
    }

    public String typeName()
    { // 1, string
        return typeName;
    }

    public int dataType()
    { // 2, int
        return dataType;
    }

    public int precision()
    { // 3, int
        return precision;
    }

    public String literalPrefix()
    { // 4, string
        return literalPrefix;
    }

    public String literalSuffix()
    { // 5, string
        return literalSuffix;
    }

    public String createParams()
    { // 6, string
        return createParams;
    }

    public short nullable()
    { // 7, short
        return nullable;
    }

    public boolean caseSensitive()
    { // 8, boolean
        return caseSensitive;
    }

    public short searchable()
    { // 9, short
        return searchable;
    }

    public boolean unsignedAttribute()
    { // 10, boolean
        return unsignedAttr;
    }

    public boolean fixedPrecScale()
    { // 11, boolean
        return fixedPrecScale;
    }

    public boolean autoIncrement()
    { // 12, boolean
        return autoIncrement;
    }

    public String localTypeName()
    { // 13, string
        return localTypeName;
    }

    public short minimunScale()
    { // 14, short
        return minimumScale;
    }

    public short maximumScale()
    { // 15, short
        return maximumScale;
    }

    public int sqlDataType()
    { // 16, int
        return sqlDataType;
    }

    public int sqlDatetimeSub()
    { // 17, int
        return sqlDatetimeSub;
    }

    public int numPrecRadix()
    { // 18, int
        return numPrecRadix;
    }
}
