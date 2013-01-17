package com.treasure_data.jdbc.command;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.TDConnection;
import com.treasure_data.jdbc.TDResultSet;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.logger.Config;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobResult2;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.TableSummary;

public class TDClientAPI implements ClientAPI {
    private static final Logger LOG = Logger.getLogger(TDClientAPI.class.getName());

    private TreasureDataClient client;

    private Properties props;

    private Database database;

    private int maxRows = 5000;

    public TDClientAPI(TDConnection conn) {
        this(new TreasureDataClient(conn.getProperties()), conn.getProperties(),
                conn.getDatabase(), conn.getMaxRows());
    }

    public TDClientAPI(TreasureDataClient client, Properties props, Database database) {
        this(client, props, database, 5000);
    }

    public TDClientAPI(TreasureDataClient client, Properties props, Database database, int maxRows) {
        this.client = client;
        this.props = props;
        this.database = database;
        this.maxRows = maxRows;

        checkCredentials();

        {
            Properties sysprops = System.getProperties();
            if (sysprops.getProperty(Config.TD_LOGGER_AGENTMODE) == null) {
                sysprops.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
            }
            if (sysprops.getProperty(Config.TD_LOGGER_API_KEY) == null) {
                String apiKey = client.getTreasureDataCredentials().getAPIKey();
                sysprops.setProperty(Config.TD_LOGGER_API_KEY, apiKey);
            }
        }
    }

    private void checkCredentials() {
        String apiKey = client.getTreasureDataCredentials().getAPIKey();
        if (apiKey != null) {
            return;
        }

        if (props == null) {
            return;
        }

        String user = props.getProperty("user");
        String password = props.getProperty("password");
        try {
            client.authenticate(new AuthenticateRequest(user, password));
        } catch (ClientException e) {
            LOG.throwing(this.getClass().getName(), "checkCredentials", e);
            return;
        }
    }

    public List<DatabaseSummary> showDatabases() throws ClientException {
        return client.listDatabases();
    }

    public List<TableSummary> showTables() throws ClientException {
        return client.listTables(database);
    }

    public boolean drop(String table) throws ClientException {
        client.deleteTable(database.getName(), table);
        return true;
    }

    public boolean create(String table) throws ClientException {
        client.createTable(database, table);
        return true;
    }

    public boolean insert(String tableName, Map<String, Object> record)
            throws ClientException {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        return logger.log(tableName, record);
    }

    public TDResultSetBase select(String sql) throws ClientException {
        return select(sql, 0);
    }

    public TDResultSetBase select(String sql, int queryTimeout) throws ClientException {
        TDResultSetBase rs = null;

        Job job = new Job(database, sql);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = client.submitJob(request);
        job = result.getJob();

        if (job != null) {
            rs = new TDResultSet(this, maxRows, job, queryTimeout);
        }
        return rs;
    }

    public JobSummary waitJobResult(Job job) throws ClientException {
        ShowJobResult result;
        while (true) {
            ShowJobRequest request = new ShowJobRequest(job);
            result = client.showJob(request);

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Job status: " + result.getJob().getStatus());
            }

            JobSummary.Status stat = result.getJob().getStatus();
            if (stat == JobSummary.Status.SUCCESS) {
                break;
            } else if (stat == JobSummary.Status.ERROR) {
                throw new ClientException("job status: error");
            } else if (stat == JobSummary.Status.KILLED) {
                throw new ClientException("job status: killed");
            }

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                return null;
            }
        }
        return result.getJob();
    }

    public Unpacker getJobResult(Job job) throws ClientException {
        GetJobResultRequest request = new GetJobResultRequest(new JobResult(job));
        GetJobResultResult result = client.getJobResult(request);
        return result.getJobResult().getResult();
    }

    public Unpacker getJobResult2(Job job) throws ClientException {
        GetJobResultRequest request = new GetJobResultRequest(new JobResult2(job));
        GetJobResultResult result = client.getJobResult(request);
        return result.getJobResult().getResult();
    }

    public boolean flush() {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
        return true;
    }

    public void close() throws ClientException {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
    }
}
