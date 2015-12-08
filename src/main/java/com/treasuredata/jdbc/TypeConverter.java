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
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.sql.Date;
import java.sql.Timestamp;

/**
 *
 */
public class TypeConverter
{
    public static boolean convertToBoolean(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return false;
            case INTEGER:
            case FLOAT:
                return v.asNumberValue().toInt() != 0;
            case BOOLEAN:
                return v.asBooleanValue().getBoolean();
            case BINARY:
            case STRING:
                return Boolean.getBoolean(v.asRawValue().toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into boolean", v));
        }
    }

    public static int convertToInt(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return 0;
            case INTEGER:
            case FLOAT:
                return v.asNumberValue().toInt();
            case BOOLEAN:
                return v.asBooleanValue().getBoolean() ? 1 : 0;
            case BINARY:
            case STRING:
                return Integer.parseInt(v.asRawValue().toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into int", v));
        }
    }

    public static long convertToLong(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return 0L;
            case INTEGER:
            case FLOAT:
                return v.asNumberValue().toLong();
            case BOOLEAN:
                return v.asBooleanValue().getBoolean() ? 1L : 0L;
            case BINARY:
            case STRING:
                return Long.parseLong(v.asRawValue().toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into long", v));
        }
    }

    public static float convertToFloat(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return 0;
            case INTEGER:
            case FLOAT:
                return v.asNumberValue().toFloat();
            case BOOLEAN:
                return v.asBooleanValue().getBoolean() ? 1f : 0f;
            case BINARY:
            case STRING:
                return Float.parseFloat(v.asRawValue().toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into float", v));
        }
    }

    public static double convertToDouble(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return 0;
            case INTEGER:
            case FLOAT:
                return v.asNumberValue().toDouble();
            case BOOLEAN:
                return v.asBooleanValue().getBoolean() ? 1 : 0;
            case BINARY:
            case STRING:
                return Double.parseDouble(v.asRawValue().toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into double", v));
        }
    }

    public static Timestamp convertToTimestamp(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return null;
//            // TODO for timestamp type (-1)
//            case EXTENSION:
//                ExtensionValue ev = v.asExtensionValue();
//                if(ev.getType() == -1) {
//                    byte[] data = ev.getData();
//                    // byte array -> timestamp
//                }
            case BINARY:
            case STRING:
                return Timestamp.valueOf(v.toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into timestamp", v));
        }
    }

    public static Date convertToDate(Value v) {
        ValueType vt = v.getValueType();
        switch(vt) {
            case NIL:
                return null;
//            // TODO for timestamp type (-1)
//            case EXTENSION:
//                ExtensionValue ev = v.asExtensionValue();
//                if(ev.getType() == -1) {
//                    byte[] data = ev.getData();
//                    // byte array -> timestamp
//                }
            case BINARY:
            case STRING:
                return Date.valueOf(v.toString());
            default:
                throw new MessageTypeException(String.format("Cannot convert value %v into date", v));
        }
    }


}
