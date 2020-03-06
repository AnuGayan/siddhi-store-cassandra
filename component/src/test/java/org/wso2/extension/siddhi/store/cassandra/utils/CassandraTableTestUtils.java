/*
*  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.extension.siddhi.store.cassandra.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.cassandra.exception.CassandraTableException;

import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.DEFAULT_KEY_SPACE;

public class CassandraTableTestUtils {

    private static final String HOST = "localhost";
    public static final String KEY_SPACE = "AnalyticsFamily";
    private static final Log LOG = LogFactory.getLog(CassandraTableTestUtils.class);
    public static final String PASSWORD = "";
    public static final String TABLE_NAME = "CassandraTestTable";
    public static final String USER_NAME = "";
    private static Session session;
    private static String hostIp;
    private static int port;

    public static void initializeTable() {
        dropTable();
    }

    private static void initHostAndPort() {
        hostIp = System.getenv("DOCKER_HOST_IP");
        if (hostIp == null || hostIp.isEmpty()) {
            hostIp = HOST;
        }
        String portString = System.getenv("PORT");
        if (portString == null || portString.isEmpty()) {
            port = 9042;
        } else {
            port = Integer.parseInt(portString);
        }
    }

    public static String getHostIp() {
        return hostIp;
    }

    public static int getPort() {
        return port;
    }

    private static void createConn() {
        //creating Cluster object
        initHostAndPort();
        Cluster cluster = Cluster.builder().addContactPoint(getHostIp()).withPort(getPort()).
                withCredentials(USER_NAME, PASSWORD).build();
        //Creating Session object
        session = cluster.connect();
        String analyticsFamilyKeyspace = "CREATE  KEYSPACE IF NOT EXISTS " + KEY_SPACE + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor' : 1}";
        String wso2SpKeyspace = "CREATE  KEYSPACE IF NOT EXISTS " + DEFAULT_KEY_SPACE + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor' : 1}";
        session.execute(analyticsFamilyKeyspace);
        session.execute(wso2SpKeyspace);
        session = cluster.connect(KEY_SPACE);
    }

    private static void dropTable() throws InvalidQueryException {
        createConn();
        boolean isFailed = true;
        for (int i = 0; (i < 10) && isFailed; i++) {
            isFailed = dropTableRetry();
        }
        session.close();
    }

    /*private static void dropTableRetry(int retryIndex, boolean isExecuted) throws InvalidQueryException {
        String deleteQuery = "DROP TABLE IF EXISTS " + KEY_SPACE + "." + TABLE_NAME;
        while (!isExecuted && retryIndex < 10) {
            try {
                session.execute(deleteQuery);
                isExecuted = true;
            } catch (OperationTimedOutException | NoHostAvailableException ex) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new CassandraTableException("Problem in table detetion..");
                }
                LOG.info("Retried : " + retryIndex + " " + isExecuted);
                dropTableRetry(++retryIndex, false);
            }
        }
        if (session != null) {
            session.close();
        }
    }*/

    private static boolean dropTableRetry() throws InvalidQueryException {
        String deleteQuery = "DROP TABLE IF EXISTS " + KEY_SPACE + "." + TABLE_NAME;
        try {
            session.execute(deleteQuery);
            return false;
        } catch (NoHostAvailableException ex) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new CassandraTableException("Problem in table detetion..");
            }
            LOG.info("Retried : ");
            return true;
        }
    }

    public static long getRowsInTable() {
        createConn();
        String deleteQuery = "SELECT count(*) FROM " + KEY_SPACE + "." + TABLE_NAME;
        ResultSet rs = session.execute(deleteQuery);
        long count = rs.one().getLong("count");
        session.close();
        return count;
    }

}
