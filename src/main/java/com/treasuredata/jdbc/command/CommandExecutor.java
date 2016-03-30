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
package com.treasuredata.jdbc.command;

import com.treasure_data.client.ClientException;
import com.treasuredata.jdbc.Constants;
import com.treasuredata.jdbc.OldTDResultSetBase;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 */
public class CommandExecutor
        implements Constants
{
    private static final Logger LOG = Logger.getLogger(CommandExecutor.class.getName());

    private ClientAPI api;

    public CommandExecutor(ClientAPI api)
    {
        this.api = api;
    }

    public ClientAPI getAPI()
    {
        return api;
    }

    public synchronized void execute(CommandContext context)
            throws SQLException
    {
        String sql = context.sql;
        try {
            if (sql.toUpperCase().equals("SELECT 1")) {
                context.resultSet = new TDResultSetSelectOne(api);
            }
            else {
                context.resultSet = api.select(context.sql, context.queryTimeout);
            }
        }
        catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    public static class TDResultSetSelectOne
            extends OldTDResultSetBase
    {
        private ClientAPI api;

        private int rowsFetched = 0;

        private Unpacker fetchedRows;

        private Iterator<Value> fetchedRowsItr;

        public TDResultSetSelectOne(ClientAPI api)
        {
            this.api = api;
        }

        @Override
        public boolean next()
                throws SQLException
        {
            try {
                if (fetchedRows == null) {
                    fetchedRows = fetchRows();
                    fetchedRowsItr = fetchedRows.iterator();
                }

                if (!fetchedRowsItr.hasNext()) {
                    return false;
                }

                ArrayValue vs = (ArrayValue) fetchedRowsItr.next();
                row = new ArrayList<Object>(vs.size());
                for (int i = 0; i < vs.size(); i++) {
                    row.add(i, vs.get(i));
                }
                rowsFetched++;
            }
            catch (Exception e) {
                throw new SQLException("Error retrieving next row", e);
            }
            // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
            return true;
        }

        @Override
        public void close()
                throws SQLException
        {
            // do nothing
        }

        @Override
        public ResultSetMetaData getMetaData()
                throws SQLException
        {
            return api.getMetaDataWithSelect1();
        }

        private Unpacker fetchRows()
                throws SQLException
        {
            try {
                MessagePack msgpack = new MessagePack();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Packer packer = msgpack.createPacker(out);
                List<Integer> src = new ArrayList<Integer>();
                src.add(1);
                packer.write(src);
                byte[] bytes = out.toByteArray();
                ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                Unpacker unpacker = msgpack.createUnpacker(in);
                return unpacker;
            }
            catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }
}
