package com.treasure_data.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.json.simple.JSONValue;
import org.mortbay.util.ajax.JSON;
import org.msgpack.packer.Packer;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TreasureDataQueryResultSet extends TreasureDataBaseResultSet {
    private static Logger LOG = Logger.getLogger(
            TreasureDataQueryResultSet.class.getName());

    private TreasureDataClient client;

    private SerDe serde;

    private int maxRows = 0;

    private int rowsFetched = 0;

    private int fetchSize = 50;

    private Unpacker fetchedRows;

    private Iterator<Value> fetchedRowsItr;

    private Job job;

    public TreasureDataQueryResultSet(TreasureDataClient client, int maxRows, Job job)
            throws SQLException {
        this.client = client;
        this.maxRows = maxRows;
        this.job = job;
        //initSerDe(); // TODO #MN
        //row = Arrays.asList(new Object[columnNames.size()]); // TODO #MN
    }

    public TreasureDataQueryResultSet(TreasureDataClient client, Job job)
            throws SQLException {
        this(client, 0, job);
    }

    /**
     * Instantiate the serde used to deserialize the result rows.
     */
    private void initSerDe() throws SQLException {
        // TODO #MN
        try {
            Schema fullSchema = null;
            //Schema fullSchema = client.getSchema(); // TODO #MN
            List<FieldSchema> schema = fullSchema.getFieldSchemas();
            columnNames = new ArrayList<String>();
            columnTypes = new ArrayList<String>();
            StringBuilder namesSb = new StringBuilder();
            StringBuilder typesSb = new StringBuilder();

            if ((schema != null) && (!schema.isEmpty())) {
                for (int pos = 0; pos < schema.size(); pos++) {
                    if (pos != 0) {
                        namesSb.append(",");
                        typesSb.append(",");
                    }
                    columnNames.add(schema.get(pos).getName());
                    columnTypes.add(schema.get(pos).getType());
                    namesSb.append(schema.get(pos).getName());
                    typesSb.append(schema.get(pos).getType());
                }
            }
            String names = namesSb.toString();
            String types = typesSb.toString();

            serde = new LazySimpleSerDe();
            Properties props = new Properties();
            if (names.length() > 0) {
                LOG.fine("Column names: " + names);
                props.setProperty(Constants.LIST_COLUMNS, names);
            }
            if (types.length() > 0) {
                LOG.fine("Column types: " + types);
                props.setProperty(Constants.LIST_COLUMN_TYPES, types);
            }
            serde.initialize(new Configuration(), props);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Could not create ResultSet: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        client = null;
    }

    /**
     * Moves the cursor down one row from its current position.
     *
     * @see java.sql.ResultSet#next()
     * @throws SQLException     if a database access error occurs.
     */
    public boolean next() throws SQLException {
        if (maxRows > 0 && rowsFetched >= maxRows) {
            return false;
        }

        try {
            if (fetchedRows == null) {
                fetchedRows = fetchRows(fetchSize);
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

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Fetched row: " + row);
            }
        } catch (Exception e) {
            throw new SQLException("Error retrieving next row", e);
        }
        // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
        return true;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
//        // FIXME #MN for debug
//        columnNames = new ArrayList<String>();
//        columnTypes = new ArrayList<String>();
//        columnNames.add("id");
//        columnNames.add("name");
//        columnNames.add("score");
//        columnTypes.add("string");
//        columnTypes.add("string");
//        columnTypes.add("string");
//        return super.getMetaData();
        while (true) {
            try {
                ShowJobRequest request = new ShowJobRequest(job);
                ShowJobResult result = client.showJob(request);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Job status: " + result.getJob().getStatus());
                }
                if (result.getJob().getStatus() == Job.Status.SUCCESS) {
                    initColumnNamesAndTypes(result.getJob());
                    break;
                }
            } catch (ClientException e) {
                throw new SQLException(e);
            }

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) { // ignore
            }
        }
        return super.getMetaData();
    }

    private Unpacker fetchRows(int fetchSize) throws ClientException {
//        // FIXME #MN for debug
//        org.msgpack.MessagePack msgpack = new org.msgpack.MessagePack();
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        Packer packer = msgpack.createPacker(out);
//        for (int i = 0; i < 30; i++) {
//            List<String> row = new ArrayList<String>();
//            row.add("" + i);
//            row.add("muga:" + i);
//            row.add("" + ((int) (Math.random() * i * 1000)) % 100);
//            try {
//                packer.write(row);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        byte[] bytes = out.toByteArray();
//        return msgpack.createUnpacker(new ByteArrayInputStream(bytes));
        while (true) {
            ShowJobRequest request = new ShowJobRequest(job);
            ShowJobResult result = client.showJob(request);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Job status: " + result.getJob().getStatus());
            }
            if (result.getJob().getStatus() == Job.Status.SUCCESS) {
                initColumnNamesAndTypes(result.getJob());
                break;
            }

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) { // ignore
            }
        }

        try {
            GetJobResultRequest request = new GetJobResultRequest(new JobResult(job));
            GetJobResultResult result = client.getJobResult(request);
            return result.getJobResult().getResult();
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    private void initColumnNamesAndTypes(Job job) {
        String resultSchema = job.getResultSchema();
        List<List<String>> cols = (List<List<String>>) JSONValue.parse(resultSchema);
        if (cols == null) {
            return;
        }
        columnNames = new ArrayList<String>(cols.size());
        columnTypes = new ArrayList<String>(cols.size());
        for (List<String> col : cols) {
            columnNames.add(col.get(0));
            columnTypes.add(col.get(1));
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

}
