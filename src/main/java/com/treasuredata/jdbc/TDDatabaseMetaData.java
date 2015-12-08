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

import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.jdbc.model.TDDataType;
import com.treasuredata.jdbc.model.TDImportedKey;
import org.json.simple.JSONValue;

import java.sql.*;
import java.util.*;

import static com.treasuredata.jdbc.TDJDBCException.UNSUPPORTED;

public class TDDatabaseMetaData
    implements DatabaseMetaData, Constants {
    private final TDClient client;
    private final String database;

    public TDDatabaseMetaData(TDClient client) {
        this("default", client);
    }

    public TDDatabaseMetaData(String database, TDClient client) {
        this.client = client;
        this.database = database;
    }

    public boolean allProceduresAreCallable()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean allTablesAreSelectable()
        throws SQLException {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean dataDefinitionCausesTransactionCommit()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean dataDefinitionIgnoredInTransactions()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean deletesAreDetected(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean doesMaxRowSizeIncludeBlobs()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getAttributes(String catalog, String schemaPattern,
                                   String typeNamePattern, String attributeNamePattern)
        throws SQLException
    {
        throw UNSUPPORTED();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema,
                                          String table, int scope, boolean nullable)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getCatalogSeparator()
        throws SQLException {
        return ".";
    }

    public String getCatalogTerm()
        throws SQLException {
        return "database";
    }

    public ResultSet getCatalogs()
        throws SQLException {

        List<TDDatabase> databases = client.listDatabases();
        List<String> names = Arrays.asList("TABLE_CAT");
        List<String> types = Arrays.asList("STRING");

        try {
            ResultSet result = new TDMetaDataResultSet<TDDatabase>(names,
                types, databases) {
                private int cnt = 0;

                public boolean next()
                    throws SQLException {
                    if(cnt >= data.size()) {
                        return false;
                    }

                    TDDatabase d = data.get(cnt);
                    List<Object> a = new ArrayList<Object>(1);
                    a.add(d.getName()); // TABLE_CAT String => table
                    // catalog (may be null)
                    row = a;
                    cnt++;
                    return true;
                }
            };
            return result;
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getClientInfoProperties()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getColumnPrivileges(String catalog, String schema,
                                         String table, String columnNamePattern)
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Convert a pattern containing JDBC catalog search wildcards into Java
     * regex patterns.
     *
     * @param pattern input which may contain '%' or '_' wildcard characters, or
     *                these characters escaped using
     *                {@link #getSearchStringEscape()}.
     * @return replace %/_ with regex search characters, also handle escaped
     * characters.
     */
    private String convertPattern(final String pattern) {
        if(pattern == null) {
            return ".*";
        }
        else {
            StringBuilder result = new StringBuilder(pattern.length());

            boolean escaped = false;
            for(int i = 0, len = pattern.length(); i < len; i++) {
                char c = pattern.charAt(i);
                if(escaped) {
                    if(c != '\\') {
                        escaped = false;
                    }
                    result.append(c);
                }
                else {
                    if(c == '\\') {
                        escaped = true;
                        continue;
                    }
                    else if(c == '%') {
                        result.append(".*");
                    }
                    else if(c == '_') {
                        result.append('.');
                    }
                    else {
                        result.append(c);
                    }
                }
            }

            return result.toString();
        }
    }

    @SuppressWarnings("unchecked")
    public ResultSet getColumns(String catalog, final String schemaPattern,
                                final String tableNamePattern, final String columnNamePattern)
        throws SQLException {
        if(catalog == null) {
            catalog = "default";
        }

        String tableNamePattern1 = convertPattern(tableNamePattern);
        String columnNamePattern1 = convertPattern(columnNamePattern);

        List<com.treasuredata.client.model.TDTable> tableList = client.listTables(database);
        List<TDColumn> columns = new ArrayList<TDColumn>();
        for(TDTable t : tableList) {
            if(!t.getName().matches(tableNamePattern1)) {
                continue;
            }

            boolean hasTimeColumn = false;
            int ordinal = 1;
            for(TDColumn col : t.getColumns()) {
                if(col.getName().equals("time")) {
                    hasTimeColumn = true;
                }

                if(!col.getName().matches(columnNamePattern1)) {
                    continue;
                }

                TDColumn c = new TDColumn(col., t.getName(), catalog, ftype,
                    "comment", ordinal);
                columns.add(c);
                ordinal++;
            }

            if(t.getType() == Table.Type.LOG && !hasTimeColumn && "time".matches(columnNamePattern1)) {
                TDColumn c = new TDColumn("time", t.getName(), catalog, "int", "comment", ordinal);
                columns.add(c);
                ordinal++;
            }
        }
        Collections.sort(columns, new Comparator<TDColumn>() {
            /**
             * We sort the output of getColumns to guarantee jdbc compliance.
             * First check by table name then by ordinal position
             */
            public int compare(TDColumn o1, TDColumn o2) {
                int nameDiff = o1.getTableName().compareTo(o2.getTableName());
                return nameDiff != 0 ? nameDiff : o1.getOrdinal() - o2.getOrdinal();
            }
        });

        List<String> names = Arrays.asList("TABLE_CAT", "TABLE_SCHEM",
            "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
            "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
            "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
            "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG",
            "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
            "IS_AUTOINCREMENT");

        List<String> types = Arrays.asList("STRING", // TABLE_CAT
            "STRING", // TABLE_SCHEM
            "STRING", // TABLE_NAME
            "STRING", // COLUMN_NAME
            "INT", // DATA_TYPE
            "STRING", // TYPE_NAME
            "INT", // COLUMN_SIZE
            "INT", // BUFFER_LENGTH
            "INT", // DECIMAL_DIGITS
            "INT", // NUM_PREC_RADIX
            "INT", // NULLABLE
            "STRING", // REMARKS
            "STRING", // COLUMN_DEF
            "INT", // SQL_DATA_TYPE
            "INT", // SQL_DATEIME_SUB
            "INT", // CHAR_OCTET_LENGTH
            "INT", // ORDINAL_POSITION
            "STRING", // IS_NULLABLE
            "STRING", // SCOPE_CATLOG
            "STRING", // SCOPE_SCHEMA
            "STRING", // SCOPE_TABLE
            "INT", // SOURCE_DATA_TYPE
            "STRING" // IS_AUTOINCREMENT
        );

        try {
            return new TDMetaDataResultSet<TDColumn>(names, types, columns) {
                private int cnt = 0;

                public boolean next()
                    throws SQLException {
                    if(cnt >= data.size()) {
                        return false;
                    }

                    TDColumn column = data.get(cnt);
                    List<Object> a = new ArrayList<Object>(23);
                    a.add(column.getTableCatalog()); // TABLE_CAT String =>
                    // table catalog (may be
                    // null)
                    a.add(null); // TABLE_SCHEM String => table schema (may be
                    // null)
                    a.add(column.getTableName()); // TABLE_NAME String => table
                    // name
                    a.add(column.getColumnName()); // COLUMN_NAME String =>
                    // column name
                    a.add(column.getSqlType()); // DATA_TYPE short => SQL type
                    // from java.sql.Types
                    a.add(column.getType()); // TYPE_NAME String => Data source
                    // dependent type name.
                    a.add(column.getColumnSize()); // COLUMN_SIZE int => column
                    // size.
                    a.add(null); // BUFFER_LENGTH is not used.
                    a.add(column.getDecimalDigits()); // DECIMAL_DIGITS int =>
                    // number of fractional
                    // digits
                    a.add(column.getNumPrecRadix()); // NUM_PREC_RADIX int =>
                    // typically either 10 or 2
                    a.add(DatabaseMetaData.columnNullable); // NULLABLE int =>
                    // is NULL allowed?
                    a.add(column.getComment()); // REMARKS String => comment
                    // describing column (may be
                    // null)
                    a.add(null); // COLUMN_DEF String => default value (may be
                    // null)
                    a.add(null); // SQL_DATA_TYPE int => unused
                    a.add(null); // SQL_DATETIME_SUB int => unused
                    a.add(null); // CHAR_OCTET_LENGTH int
                    a.add(column.getOrdinal()); // ORDINAL_POSITION int
                    a.add("YES"); // IS_NULLABLE String
                    a.add(null); // SCOPE_CATLOG String
                    a.add(null); // SCOPE_SCHEMA String
                    a.add(null); // SCOPE_TABLE String
                    a.add(null); // SOURCE_DATA_TYPE short
                    a.add("NO"); // IS_AUTOINCREMENT String

                    row = a;
                    cnt++;
                    return true;
                }
            };
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    public Connection getConnection()
        throws SQLException {
        throw new SQLException(new UnsupportedOperationException(
            "TDDatabaseMetaData#getConnection()"));
    }

    public ResultSet getCrossReference(String primaryCatalog,
                                       String primarySchema, String primaryTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable)
        throws SQLException {
        throw new SQLException(
            new UnsupportedOperationException(
                "TDDatabaseMetaData#getCrossReference(String, String, String, String, String, String)"));
    }

    public int getJDBCMajorVersion()
        throws SQLException {
        return JDBC_MAJOR_VERSION;
    }

    public int getJDBCMinorVersion()
        throws SQLException {
        return JDBC_MINOR_VERSION;
    }

    public int getDatabaseMajorVersion()
        throws SQLException {
        return Constants.DATABASE_MAJOR_VERSION;
    }

    public int getDatabaseMinorVersion()
        throws SQLException {
        return Constants.DATABASE_MINOR_VERSION;
    }

    public String getDatabaseProductName()
        throws SQLException {
        return DATABASE_NAME;
    }

    public String getDatabaseProductVersion()
        throws SQLException {
        return DATABASE_FULL_VERSION;
    }

    public int getDriverMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    public int getDriverMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    public String getDriverName()
        throws SQLException {
        return TreasureDataDriver.class.getName();
    }

    public String getDriverVersion()
        throws SQLException {
        return DRIVER_FULL_VERSION;
    }

    public int getDefaultTransactionIsolation()
        throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getExtraNameCharacters()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
                                        String arg3)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getFunctions(String arg0, String arg1, String arg2)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getIdentifierQuoteString()
        throws SQLException {
        return "`";
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table)
        throws SQLException {
        try {
            return new TDMetaDataResultSet<TDImportedKey>(null, null, null) {
                public boolean next()
                    throws SQLException {
                    return false;
                }
            };
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate)
        throws SQLException {
        return new TDMetaDataResultSet(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"),
            Arrays.asList("STRING", "STRING", "STRING", "BOOLEAN", "STRING", "STRING", "SHORT", "SHORT", "STRING", "STRING", "INT", "INT", "STRING"),
            null) {
            public boolean next() {
                return false;
            }
        };
    }

    public int getMaxBinaryLiteralLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * catalog name.
     */
    public int getMaxCatalogNameLength()
        throws SQLException {
        return MAX_CATALOG_NAME_LENGTH;
    }

    public int getMaxCharLiteralLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of characters this database allows for a
     * column name.
     */
    public int getMaxColumnNameLength()
        throws SQLException {
        return MAX_COLUMN_NAME_LENGTH;
    }

    public int getMaxColumnsInGroupBy()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public int getMaxColumnsInIndex()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public int getMaxColumnsInOrderBy()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of columns this database allows in a
     * <code>SELECT</code> list.
     */
    public int getMaxColumnsInSelect()
        throws SQLException {
        return MAX_COLUMNS_IN_SELECT;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a table.
     */
    public int getMaxColumnsInTable()
        throws SQLException {
        return MAX_COLUMNS_IN_TABLE;
    }

    public int getMaxConnections()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public int getMaxCursorNameLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public int getMaxIndexLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public int getMaxProcedureNameLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of bytes this database allows in a single
     * row.
     */
    public int getMaxRowSize()
        throws SQLException {
        return MAX_ROW_SIZE;
    }

    public int getMaxSchemaNameLength()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of characters this database allows in an SQL
     * statement.
     */
    public int getMaxStatementLength()
        throws SQLException {
        return MAX_STATEMENT_LENGTH;
    }

    public int getMaxStatements()
        throws SQLException {
        throw UNSUPPORTED();
    }

    /**
     * Retrieves the maximum number of characters this database allows in a
     * table name.
     */
    public int getMaxTableNameLength()
        throws SQLException {
        return MAX_TABLE_NAME_LENGTH;
    }

    /**
     * Retrieves the maximum number of tables this database allows in a
     * <code>SELECT</code> statement.
     */
    public int getMaxTablesInSelect()
        throws SQLException {
        return MAX_TABLES_IN_SELECT;
    }

    /**
     * Retrieves the maximum number of characters this database allows in a user
     * name.
     */
    public int getMaxUserNameLength()
        throws SQLException {
        return MAX_USER_NAME_LENGTH;
    }

    public String getNumericFunctions()
        throws SQLException {
        return "";
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
        throws SQLException {
        return new TDMetaDataResultSet(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
            Arrays.asList("STRING", "STRING", "STRING", "STRING", "SHORT", "STRING"),
            null) {
            public boolean next() {
                return false;
            }
        };
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getProcedureTerm()
        throws SQLException {
        return "UDF";
    }

    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern)
        throws SQLException {
        return null;
    }

    public int getResultSetHoldability()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public RowIdLifetime getRowIdLifetime()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getSQLKeywords()
        throws SQLException {
        return "TRUE,FALSE,ALL,AND,OR,NOT,LIKE,ASC,DESC,ORDER,BY,GROUP,WHERE,"
            + "FROM,AS,SELECT,DISTINCT,INSERT,OVERWRITE,OUTER,JOIN,LEFT,RIGHT,"
            + "FULL,ON,PARTITION,PARTITIONS,TABLE,TABLES,TBLPROPERTIES,SHOW,MSCK,"
            + "DIRECTORY,LOCAL,TRANSFORM,USING,CLUSTER,DISTRIBUTE,SORT,UNION,LOAD,"
            + "DATA,INPATH,IS,NULL,CREATE,EXTERNAL,ALTER,DESCRIBE,DROP,REANME,TO,"
            + "COMMENT,BOOLEAN,TINYINT,SMALLINT,INT,BIGINT,FLOAT,DOUBLE,DATE,"
            + "DATETIME,TIMESTAMP,STRING,BINARY,ARRAY,MAP,REDUCE,PARTITIONED,"
            + "CLUSTERED,SORTED,INTO,BUCKETS,ROW,FORMAT,DELIMITED,FIELDS,TERMINATED,"
            + "COLLECTION,ITEMS,KEYS,LINES,STORED,SEQUENCEFILE,TEXTFILE,INPUTFORMAT,"
            + "OUTPUTFORMAT,LOCATION,TABLESAMPLE,BUCKET,OUT,OF,CAST,ADD,REPLACE,"
            + "COLUMNS,RLIKE,REGEXP,TEMPORARY,FUNCTION,EXPLAIN,EXTENDED,SERDE,WITH,"
            + "SERDEPROPERTIES,LIMIT,SET,TBLPROPERTIES";
    }

    public int getSQLStateType()
        throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
    }

    public String getSchemaTerm()
        throws SQLException {
        return "";
    }

    public ResultSet getSchemas()
        throws SQLException {
        return getSchemas(null, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ResultSet getSchemas(String catalog, String schemaPattern)
        throws SQLException {
        return new TDMetaDataResultSet(Arrays.asList("TABLE_SCHEM",
            "TABLE_CATALOG"), Arrays.asList("STRING", "STRING"), null) {

            public boolean next()
                throws SQLException {
                return false;
            }
        };
    }

    public String getSearchStringEscape()
        throws SQLException {
        return String.valueOf('\\');
    }

    public String getStringFunctions()
        throws SQLException {
        return "";
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern,
                                    String tableNamePattern)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                   String typeNamePattern)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getSystemFunctions()
        throws SQLException {
        return "";
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getTableTypes()
        throws SQLException {
        List<String> names = Arrays.asList("TABLE_TYPE");
        List<String> types = Arrays.asList("STRING");
        List<TDTable.Type> data0 = Arrays.asList(TDTable.Type.values());
        ResultSet result = new TDMetaDataResultSet<TDTable.Type>(names, types,
            data0) {
            private int cnt = 0;

            public boolean next()
                throws SQLException {
                if(cnt < data.size()) {
                    List<Object> a = new ArrayList<Object>(1);
                    a.add(toTDTableType(data.get(cnt).name()));
                    row = a;
                    cnt++;
                    return true;
                }
                else {
                    return false;
                }
            }
        };
        return result;
    }

    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types)
        throws SQLException {
        /*
         * finds table descriptions in the specified database.
         * if catalog is not same as the specified database, it returns null.
         * if catalog is null or catalog is empty, catalog is set to the specified database.
         */
        if(catalog == null || catalog.isEmpty()) {
            catalog = database;
        }
        else if(!catalog.equals(database)) {
            return getEmptyTables();
        }

        /*
         * ignores schemaPattern for now. it is not used for searching table descriptions.
         */
        List<TDTable> tableList = client.listTables(catalog);


        List<TableSummary> ts = null;
        try {
            ts = api.showTables();
            if(ts == null) {
                ts = new ArrayList<TableSummary>();
            }
        }
        catch(ClientException e) {
            throw new SQLException(e);
        }

        /*
         * if types is null or includes 'TABLE', it searches table descriptions in the specified database.
         * otherwise it returns null.
         */
        if(types != null) {
            boolean typeTableWanted = false;
            for(String type : types) {
                if(type.equals("TABLE")) {
                    typeTableWanted = true;
                    break;
                }
            }

            if(!typeTableWanted) {
                return getEmptyTables();
            }
        }

        String tableNamePattern1 = convertPattern(tableNamePattern);
        List<TDTable> tables = new ArrayList<TDTable>();
        for(TableSummary t : ts) {
            /*
             * tableNamePattern - a table name pattern; must match the table
             * name as it is stored in the database.
             * 
             * The table name must match the tableNamePattern regexp to be
             * accepted.
             */
            if(!t.getName().matches(tableNamePattern1)) {
                continue;
            }
            TDTable table = new TDTable(catalog, t.getName(), "TABLE", "comment");
            tables.add(table);
        }

        if(tables.isEmpty()) {
            return getEmptyTables();
        }

        Collections.sort(tables, new Comparator<TDTable>() {
            /**
             * We sort the output of getTables to guarantee JDBC compliance to
             * sort by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, and TABLE_NAME.
             *
             * In our specific case: Sorting by catalog (only one available) and
             * schema (empty) is not necessary so we sort by type, then table
             * name.
             */
            public int compare(TDTable o1, TDTable o2) {
                int compareType = o1.getType().compareTo(o2.getType());
                if(compareType == 0) {
                    return o1.getTableName().compareTo(o2.getTableName());
                }
                else {
                    return compareType;
                }
            }
        });

        List<String> nameList = Arrays.asList("TABLE_CAT", "TABLE_SCHEM",
            "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
            "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION");

        List<String> typeList = Arrays.asList(
            "STRING", // "TABLE_CAT"
            "STRING", // "TABLE_SCHEM"
            "STRING", // "TABLE_NAME"
            "STRING", // "TABLE_TYPE"
            "STRING", // "REMARKS"
            "STRING", // "TYPE_CAT"
            "STRING", // "TYPE_SCHEM"
            "STRING", // "TYPE_NAME"
            "STRING", // "SELF_REFERENCING_COL_NAME"
            "STRING"  // "REF_GENERATION"
        );

        try {
            ResultSet result = new TDMetaDataResultSet<TDTable>(nameList,
                typeList, tables) {
                private int cnt = 0;

                public boolean next()
                    throws SQLException {
                    if(cnt >= data.size()) {
                        return false;
                    }

                    TDTable t = data.get(cnt);
                    List<Object> a = new ArrayList<Object>(10);
                    a.add(t.getTableCatalog());     // TABLE_CAT String => table catalog (may be null)
                    a.add(null);                    // TABLE_SCHEM String => table schema (may be null)
                    a.add(t.getTableName());        // TABLE_NAME String => table name
                    try {
                        a.add(t.getSqlTableType()); // TABLE_TYPE String => "TABLE"
                    }
                    catch(Exception e) {
                        throw new SQLException(e);
                    }
                    a.add(t.getComment());          // REMARKS String => explanatory comment on the table
                    a.add(null);                    // TYPE_CAT String => the types catalog (may be null)
                    a.add(null);                    // TYPE_SCHEM String => the types schema (may be null)
                    a.add(null);                    // TYPE_NAME String => type name (may be null)
                    a.add(null);                    // SELF_REFERENCING_COL_NAME String => ... (may be null)
                    a.add(null);                    // REF_GENERATION String => ... (may be null)
                    row = a;
                    cnt++;
                    return true;
                }
            };
            return result;
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    private static ResultSet getEmptyTables()
        throws SQLException {
        List<String> nameList = Arrays.asList("TABLE_CAT", "TABLE_SCHEM",
            "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
            "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION");
        List<String> typeList = Arrays.asList("STRING", // "TABLE_CAT"
            "STRING", // "TABLE_SCHEM"
            "STRING", // "TABLE_NAME"
            "STRING", // "TABLE_TYPE"
            "STRING", // "REMARKS"
            "STRING", // "TYPE_CAT"
            "STRING", // "TYPE_SCHEM"
            "STRING", // "TYPE_NAME"
            "STRING", // "SELF_REFERENCING_COL_NAME"
            "STRING" // "REF_GENERATION"
        );
        List<TDTable> tables = new ArrayList<TDTable>();
        return new TDMetaDataResultSet<TDTable>(nameList, typeList, tables) {
            public boolean next()
                throws SQLException {
                return false;
            }
        };
    }

    /**
     * Translate hive table types into jdbc table types.
     *
     * @param type
     * @return
     */
    public static String toTDTableType(String type) {
        if(type == null) {
            return null;
        }
        else if(type.equals(TDTable.Type.TABLE.toString())) {
            return "TABLE";
        }
        else if(type.equals(TDTable.Type.VIEW.toString())) {
            return "VIEW";
        }
        else if(type.equals(TDTable.Type.EXTERNAL_TABLE.toString())) {
            return "EXTERNAL TABLE";
        }
        else {
            return type;
        }
    }

    public String getTimeDateFunctions()
        throws SQLException {
        return "";
    }

    public ResultSet getTypeInfo()
        throws SQLException {
        List<TDDataType> types = new ArrayList<TDDataType>();
        try {
            ResultSet result = new TDMetaDataResultSet<TDDataType>(null, null,
                types) {
                private int cnt = 0;

                public boolean next()
                    throws SQLException {
                    if(cnt >= data.size()) {
                        return false;
                    }

                    TDDataType t = data.get(cnt);
                    List<Object> a = new ArrayList<Object>(18);
                    a.add(t.typeName()); // TYPE_NAME String
                    a.add(t.dataType()); // DATA_TYPE Integer
                    a.add(t.precision()); // PRECISION Integer
                    a.add(t.literalPrefix()); // LITERAL_PREFIX String
                    a.add(t.literalSuffix()); // LITERAL_SUFFIX String
                    a.add(t.createParams()); // CREATE_PARAMS String
                    a.add(t.nullable()); // NULLABLE Short
                    a.add(t.caseSensitive()); // CASE_SENSITIVE Boolean
                    a.add(t.searchable()); // SEARCHABLE Short
                    a.add(t.unsignedAttribute()); // UNSIGNED_ATTRIBUTE Boolean
                    a.add(t.fixedPrecScale()); // FIXED_PREC_SCALE Boolean
                    a.add(t.autoIncrement()); // AUTO_INCREMENT Boolean
                    a.add(t.localTypeName()); // LOCAL_TYPE_NAME String
                    a.add(t.minimunScale()); // MINIMUM_SCALE Short
                    a.add(t.maximumScale()); // MAXIMUM_SCALE Short
                    a.add(t.sqlDataType()); // SQL_DATA_TYPE Integer
                    a.add(t.sqlDatetimeSub()); // SQL_DATETIME_SUB Integer
                    a.add(t.numPrecRadix()); // NUM_PREC_RADIX Integer
                    row = a;
                    cnt++;
                    return true;
                }
            };
            return result;
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern, int[] types)
        throws SQLException {

        return new TDMetaDataResultSet(
            Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"),
            Arrays.asList("STRING", "STRING", "STRING", "STRING", "INT",
                "STRING", "INT"), null) {

            public boolean next()
                throws SQLException {
                return false;
            }
        };
    }

    public String getURL()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public String getUserName()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean insertsAreDetected(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean isCatalogAtStart()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean isReadOnly()
        throws SQLException {
        return true;
    }

    public boolean locatorsUpdateCopy()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean nullPlusNonNullIsNull()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean nullsAreSortedAtEnd()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean nullsAreSortedAtStart()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean nullsAreSortedHigh()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean nullsAreSortedLow()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean othersDeletesAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean othersInsertsAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean othersUpdatesAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean ownDeletesAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean ownInsertsAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean ownUpdatesAreVisible(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean storesLowerCaseIdentifiers()
        throws SQLException {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean storesMixedCaseIdentifiers()
        throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean storesUpperCaseIdentifiers()
        throws SQLException {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsANSI92EntryLevelSQL()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsANSI92FullSQL()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsANSI92IntermediateSQL()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsAlterTableWithAddColumn()
        throws SQLException {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn()
        throws SQLException {
        return false;
    }

    public boolean supportsBatchUpdates()
        throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation()
        throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls()
        throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsColumnAliasing()
        throws SQLException {
        return true;
    }

    public boolean supportsConvert()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsConvert(int fromType, int toType)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsCoreSQLGrammar()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsCorrelatedSubqueries()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsDifferentTableCorrelationNames()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsExpressionsInOrderBy()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsExtendedSQLGrammar()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsFullOuterJoins()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsGetGeneratedKeys()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsGroupBy()
        throws SQLException {
        return true;
    }

    public boolean supportsGroupByBeyondSelect()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsGroupByUnrelated()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsIntegrityEnhancementFacility()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsLikeEscapeClause()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsLimitedOuterJoins()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsMinimumSQLGrammar()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsMixedCaseIdentifiers()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsMixedCaseQuotedIdentifiers()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsMultipleOpenResults()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsMultipleResultSets()
        throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsNamedParameters()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsNonNullableColumns()
        throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsOpenCursorsAcrossRollback()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsOpenStatementsAcrossCommit()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsOpenStatementsAcrossRollback()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsOrderByUnrelated()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsOuterJoins()
        throws SQLException {
        return true;
    }

    public boolean supportsPositionedDelete()
        throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate()
        throws SQLException {
        return false;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException {
        return false;
    }

    public boolean supportsResultSetType(int type)
        throws SQLException {
        return true;
    }

    public boolean supportsSavepoints()
        throws SQLException {
        return false;
    }

    public boolean supportsSchemasInDataManipulation()
        throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls()
        throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions()
        throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate()
        throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsStoredProcedures()
        throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInComparisons()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsSubqueriesInExists()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsSubqueriesInIns()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsSubqueriesInQuantifieds()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsTableCorrelationNames()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsTransactions()
        throws SQLException {
        return false;
    }

    public boolean supportsUnion()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean supportsUnionAll()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean updatesAreDetected(int type)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean usesLocalFilePerTable()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean usesLocalFiles()
        throws SQLException {
        throw UNSUPPORTED();
    }

    public boolean isWrapperFor(Class<?> iface)
        throws SQLException {
        throw UNSUPPORTED();
    }

    public <T> T unwrap(Class<T> iface)
        throws SQLException {
        throw UNSUPPORTED();
    }
}
