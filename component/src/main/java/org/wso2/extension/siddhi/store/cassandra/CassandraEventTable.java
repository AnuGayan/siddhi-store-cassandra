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

package org.wso2.extension.siddhi.store.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.CannotLoadConfigurationException;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.store.cassandra.condition.CassandraCompiledCondition;
import org.wso2.extension.siddhi.store.cassandra.condition.CassandraConditionVisitor;
import org.wso2.extension.siddhi.store.cassandra.config.CassandraStoreConfig;
import org.wso2.extension.siddhi.store.cassandra.exception.CassandraTableException;
import org.wso2.extension.siddhi.store.cassandra.util.CassandraTableUtils;
import org.wso2.extension.siddhi.store.cassandra.util.Constant;
import org.wso2.extension.siddhi.store.cassandra.util.TableMeta;
import org.wso2.extension.siddhi.store.cassandra.util.iterator.CassandraIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_ELEMENT_KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_HOST;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_USER_NAME;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CONDITION_CQL_ID;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_AND;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_EQUALS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_FILTERING;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_ID;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_TEXT;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_WHERE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.DEFAULT_KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.DEFAULT_PORT;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_COLUMNS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_COLUMNS_AND_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_CONDITION;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_INDEX;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_INSERT_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_KEYSPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_PRIMARY_KEYS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_QUESTION_MARKS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_SELECT_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_TABLE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.QUESTION_MARK;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.TABLE_PROPERTY_CLUSTERING_KEY;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.TABLE_PROPERTY_COLUMN_NAME;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.TABLE_PROPERTY_KIND;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.TABLE_PROPERTY_PARTITION_KEY;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.TABLE_PROPERTY_TYPE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.WHITESPACE;


/**
 * Class representing the Cassandra Event Table implementation.
 */
@Extension(
        name = "cassandra",
        namespace = "store",
        description = "This extension assigns data sources and connection instructions to event tables. It also " +
                "implements read-write operations on connected datasource.",
        parameters = {
                @Parameter(name = "cassandra.host",
                        description = "Host that is used to get connected in to the cassandra keyspace.",
                        type = {DataType.STRING},
                        defaultValue = "localhost"),
                @Parameter(name = "column.family",
                        description = "The name with which the event table should be persisted in the store. If no " +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi Application query."),
                @Parameter(name = "client.port",
                        description = "Client port that is used to get connected with the client store. If no " +
                                "name is specified via this parameter, the default port is taken.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "9042"),
                @Parameter(name = "keyspace",
                        description = "User need to give the keyspace that the data is persisted. " +
                                "Keyspace name is specified via this parameter.",
                        type = {DataType.STRING},
                        defaultValue = "cassandraTestTable"),
                @Parameter(name = "username",
                        description = "Through user name user can specify the relevent username " +
                                "that is used to log in to the cassandra keyspace .",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The username of the keyspace"
                ),
                @Parameter(name = "password",
                        description = "Through password user can specify the relevent password " +
                                "that is used to log in to the cassandra keyspace .",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The password of the keyspace"
                )
        },
        examples = {
                @Example(
                        syntax = "define stream StockStream (symbol string, price float, volume long); \n" +
                                "@Store(type=\'cassandra\', column.family=\'StockTable\'," +
                                "keyspace=\'AnalyticsFamily\'," +
                                "username=\'cassandra\',password=\'cassandra\',cassandra.host=\'localhost\')" +
                                "@IndexBy(\'volume\')" +
                                "@PrimaryKey(\'symbol\')" +
                                "define table StockTable (symbol string, price float, volume long); ",
                        description = "This definition creates an event table named `StockTable` in the " +
                                "AnalyticsFamily keyspace on the Cassandra instance if it does not already exist " +
                                "(with 3 attributes named `symbol`, `price`, and `volume` of the `string`, " +
                                "`float` and `long` types respectively). The connection is made as specified by the " +
                                "parameters configured for the '@Store' annotation. The `symbol` attribute is " +
                                "considered a unique field, and the values for this attribute are the " +
                                "Cassandra row IDs."
                )
        }
)
public class CassandraEventTable extends AbstractRecordTable {

    private static final Logger LOG = Logger.getLogger(CassandraEventTable.class);
    private Session session;
    private List<Attribute> schema;
    private List<Attribute> primaryKeys;
    private Annotation storeAnnotation;
    private String tableName;
    private String keyspace;
    private String host;
    private String addDataQuerySt;
    private List<Integer> objectAttributes;  // Used for object data insertion.
    private boolean noKeys;
    private String selectQuery;
    private String updateQuery;
    private Annotation indexAnnotation;
    private boolean noKeyTable;              // To check whether table does not have primary key and if it has primary
    // keys check the are matching with the persisted primary keys
    private Map<String, String> persistedKeyColumns; // column name -> data type
    private CassandraStoreConfig cassandraStoreConfig;
    private List<String> indexes;
    private int port;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.schema = tableDefinition.getAttributeList();
        this.storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        this.indexAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX_BY,
                tableDefinition.getAnnotations());
        String storeAnnotationTableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        String storeAnnotationKeyspace = storeAnnotation.getElement(ANNOTATION_ELEMENT_KEY_SPACE);
        String storeAnnotationPort = storeAnnotation.getElement(ANNOTATION_CLIENT_PORT);
        this.tableName = CassandraTableUtils.isEmpty(storeAnnotationTableName) ? tableDefinition.getId() :
                storeAnnotationTableName;
        this.host = storeAnnotation.getElement(ANNOTATION_HOST);
        this.keyspace = CassandraTableUtils.isEmpty(storeAnnotationKeyspace) ? DEFAULT_KEY_SPACE :
                storeAnnotationKeyspace;
        this.port = CassandraTableUtils.isEmpty(storeAnnotationPort) ? DEFAULT_PORT :
                Integer.parseInt(storeAnnotationPort);

        // loading cassandra config file
        try {
            this.cassandraStoreConfig = new CassandraTableUtils().readConfigFile(configReader);
        } catch (CannotLoadConfigurationException e) {
            throw new CassandraTableException("Cannot find a cassandra configuration for the keyspace "
                    + this.keyspace + e.getMessage(), e);
        }

        if (primaryKeyAnnotation == null) {
            this.noKeys = true;
            this.primaryKeys = new ArrayList<>();
            Attribute primaryKey = new Attribute(CQL_ID, Attribute.Type.STRING);
            this.primaryKeys.add(primaryKey);
        } else {
            this.primaryKeys = CassandraTableUtils.initPrimaryKeys(this.schema, primaryKeyAnnotation);
        }
        if (!this.isValidKeyspaceAndTable()) {
            throw new CassandraTableException("Invalid table name or Keyspace name. " +
                    "Please refer the cassandra documentation for naming valid table and keyspace");
        }
        buildInsertAndSelectQuery();
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        PreparedStatement preparedStatement = session.prepare(addDataQuerySt);
        for (Object[] record : records) {
            if (!objectAttributes.isEmpty()) {
                objectAttributes.forEach(columnNo -> {
                    Object oldData = record[columnNo];
                    try {
                        record[columnNo] = resolveObjectData(oldData);
                    } catch (IOException ex) {
                        throw new CassandraTableException("Error in object insertion ensure that the objects " +
                                "are serializable.", ex);
                    }
                });
            }
            addData(record, preparedStatement);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        String compiledQuery = cassandraCompiledCondition.getCompiledQuery();
        String finalSearchQuery;
        PreparedStatement preparedStatement;
        ResultSet result;
        if (compiledQuery.isEmpty()) {
            finalSearchQuery = selectQuery.replace(PLACEHOLDER_CONDITION, "").
                    replace(CQL_WHERE, "").replace(CQL_FILTERING, "");
            result = session.execute(finalSearchQuery);
        } else {
            List<String> storeAttributeNames = cassandraCompiledCondition.getStoreAttributeNames();
            boolean containsAllPrimaryKeys = containsAllPrimaryKeys(storeAttributeNames);
            if (cassandraCompiledCondition.getReadOnlyCondition() &&
                    ((containsAllPrimaryKeys(storeAttributeNames) &&
                            queryContainsOnlyOneIndex(storeAttributeNames)) || containsAllPrimaryKeys)) {
                finalSearchQuery = selectQuery.replace(PLACEHOLDER_CONDITION, compiledQuery)
                        .replace(CQL_FILTERING, "");
                preparedStatement = session.prepare(finalSearchQuery);
                //This array consists of values to be passed to the prepared statement
                Object[] argSet = constructArgSet(compiledCondition, findConditionParameterMap);
                BoundStatement bound = preparedStatement.bind(argSet);
                result = session.execute(bound);
            } else {
                finalSearchQuery = selectQuery.replace(PLACEHOLDER_CONDITION, compiledQuery);
                preparedStatement = session.prepare(finalSearchQuery);
                //This array consists of values to be passed to the prepared statement
                Object[] argSet = constructArgSet(compiledCondition, findConditionParameterMap);
                BoundStatement bound = preparedStatement.bind(argSet);
                result = session.execute(bound);
            }
        }
        return new CassandraIterator(result.iterator(), schema);
    }

    private boolean queryContainsOnlyOneIndex(List<String> storeAttributeNames) {
        if (indexes == null) {
            return false;
        }
        int noOfIndexesInFind = 0;
        for (String index : indexes) {
            if (storeAttributeNames.contains(index)) {
                noOfIndexesInFind++;
            }
            if (noOfIndexesInFind == 2) {
                return false;
            }
        }
        return noOfIndexesInFind == 1;
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        Object[] argSet = constructArgSet(compiledCondition, containsConditionParameterMap);
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String cql = cassandraStoreConfig.getRecordExistQuery().replace(PLACEHOLDER_KEYSPACE, keyspace).
                replace(PLACEHOLDER_TABLE, tableName).replace(PLACEHOLDER_CONDITION, compiledQuery);
        PreparedStatement preparedStatement = session.prepare(cql);
        BoundStatement boundStatement = preparedStatement.bind(argSet);
        ResultSet rs = session.execute(boundStatement);
        return (rs.one() != null);
    }

    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions, List<Map<String,
            Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        int i = 0;
        for (Map<String, Object> updateSetParameterMap : updateSetParameterMaps) {
            // When the user has define the primary key
            if (!(noKeyTable) && containsAllPrimaryKeys(updateConditionParameterMaps.get(i)) &&
                    ((CassandraCompiledCondition) updateCondition).getReadOnlyCondition()) {
                // if there is a match to the provided key is found update is possible
                if (contains(updateConditionParameterMaps.get(i), updateCondition)) {
                    updateSingleRow(updateSetParameterMap, updateCondition, updateConditionParameterMaps.get(i));
                } else {
                    throw new CassandraTableException("Row does not exist with the provided keys.. " +
                            "Try to update with existing keys. Update failed. ");
                }

            } else if (noKeyTable) {
                // need to search the rows and update them
                List<String> ids = findAllIDs(updateCondition, updateConditionParameterMaps.get(i));
                executeAsBatchNoIdUpdate(ids, updateSetParameterMap);
            } else {
                List<Object[]> ids = findAllUserDefinedIDs(updateCondition, updateConditionParameterMaps.get(i));
                executeAsBatchNonPrimaryKeyUpdate(ids, updateSetParameterMap);
            }
            i++;
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        deleteConditionParameterMaps.forEach(deleteConditionMap -> {
            if (!(noKeyTable) && cassandraCompiledCondition.getReadOnlyCondition() &&
                    containsAllPrimaryKeys(cassandraCompiledCondition.getStoreAttributeNames())) {
                deleteSingleRow(deleteConditionMap, compiledCondition);
            } else if (noKeyTable) {
                // need to find the key values with the column name _id
                List<String> ids = findAllIDs(compiledCondition, deleteConditionMap);
                String deleteQuery = buildQueryForNoIdDelete(ids);
                executeAsBatchNoIdDelete(ids, deleteQuery);
            } else {
                // need to find the key values defined by user in table defining
                List<Object[]> ids = findAllUserDefinedIDs(compiledCondition, deleteConditionMap);
                // Delete when user does not give the primary keys or if it is not a readonly condition
                nonPrimaryKeyDelete(ids);
            }
        });
    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        int i = 0;
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) updateCondition;
        for (Map<String, Object> updateSetParameterMap : updateSetParameterMaps) {
            if (!(noKeyTable) && cassandraCompiledCondition.getReadOnlyCondition() &&
                    containsAllPrimaryKeys(cassandraCompiledCondition.getStoreAttributeNames())) {
                updateOrAddSingleRow(updateSetParameterMap, updateCondition, updateConditionParameterMaps.get(i));
            } else if (noKeyTable) {
                // need to search the rows and update them
                List<String> ids = findAllIDs(updateCondition, updateConditionParameterMaps.get(i));
                if (ids.isEmpty()) {
                    // need to insert
                    updateOrAddToNoKeyTable(updateSetParameterMap);
                } else {
                    // need to update
                    executeAsBatchNoIdUpdate(ids, updateSetParameterMap);
                }
            } else {
                // updating key defined table
                List<Object[]> ids = findAllUserDefinedIDs(updateCondition, updateConditionParameterMaps.get(i));
                if (ids.isEmpty()) {
                    // Since we do not know the primary key this operation cannot be done
                    throw new CassandraTableException("No results found for the given values. Only update " +
                            "functionality is capable without primary keys. If update or insert operation is needed" +
                            "whole key should be included");
                } else {
                    executeAsBatchNonPrimaryKeyUpdate(ids, updateSetParameterMap);
                }
                i++;
            }

        }
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CassandraConditionVisitor visitor = new CassandraConditionVisitor();
        expressionBuilder.build(visitor);
        return new CassandraCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.getReadOnlyCondition(), visitor.getStoreAtrributeNames());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        //creating Cluster object
        String username = storeAnnotation.getElement(ANNOTATION_USER_NAME);
        String password = storeAnnotation.getElement(ANNOTATION_PASSWORD);
        Cluster cluster;
        try {
            if (username.isEmpty() && password.isEmpty()) {
                cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
            } else {
                cluster = Cluster.builder().addContactPoint(host).withPort(port).
                        withCredentials(username, password).build();
            }
            session = cluster.connect(keyspace);
        } catch (NoHostAvailableException e) {
            throw new ConnectionUnavailableException("Tried to connect to a host, but no host is found.", e);
        }
        // NoHostAvailableException: All host(s) tried for query failed   when the connection is not established..
        // Runtime error thrown
        checkTable();
        LOG.info("Store " + keyspace + "." + tableName + " is initialized");
    }

    @Override
    protected void disconnect() {
        if (session != null) {
            session.close();
        }
    }

    @Override
    protected void destroy() {
        // This is not supported in cassandra
    }

    /**
     * This method is used to convert an object to a byte array to be persisted as a blob data
     *
     * @param cellData data that is in a cell of a column
     * @return Object in a the form of bytes
     * @throws IOException exception thrown in a IO operation failure
     */
    private Object resolveObjectData(Object cellData) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(cellData);
        out.flush();
        byte[] dataToBytes = bos.toByteArray();
        bos.close();
        out.close();
        return ByteBuffer.wrap(dataToBytes);
    }

    /**
     * This method will be used to inset data to the cassandra keyspace.
     *
     * @param record            records that need to be added to the table, each Object[] represent a record and it will
     *                          match the attributes of the Table Definition passed by the add method in
     *                          CassandraEventTable.
     * @param preparedStatement prepared statement to insert data
     */
    private void addData(Object[] record, PreparedStatement preparedStatement) {
        // Need to decide whether this table has a primary key. If there is a primary key
        // then the primary key annotation should n`t be null
        BoundStatement bound;
        if (noKeyTable) {
            Object[] withPrimaryKey = Arrays.copyOf(record, record.length + 1);
            withPrimaryKey[record.length] = CassandraTableUtils.generatePrimaryKeyValue();
            bound = preparedStatement.bind(withPrimaryKey);
        } else {
            bound = preparedStatement.bind(record);
        }
        session.execute(bound);
    }

    /**
     * Find arguments to matching the compiled condition
     *
     * @param compiledCondition     map of matching StreamVariable Ids and their values
     *                              corresponding to the compiled condition
     * @param conditionParameterMap the compiledCondition against which records should be matched
     * @return Object[] of matching arguments
     */
    private Object[] constructArgSet(CompiledCondition compiledCondition,
                                     Map<String, Object> conditionParameterMap) {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        SortedMap<Integer, Object> compiledParameters = cassandraCompiledCondition.getParameters();
        Object[] argSet = new Object[compiledParameters.size()];
        int i = 0;
        for (SortedMap.Entry<Integer, Object> entry : compiledParameters.entrySet()) {
            Object parameter = entry.getValue();
            if (parameter instanceof Constant) {
                // if the value is a constant
                Constant constant = (Constant) parameter;
                argSet[i] = constant.getValue();
            } else {
                // if the value is an attribute
                Attribute variable = (Attribute) parameter;
                String attributeName = variable.getName();
                argSet[i] = conditionParameterMap.get(attributeName);
            }
            i++;
        }
        return argSet;
    }

    /**
     * This used to find all ids (Primary Key) to update or delete a row in case where
     * user has not defined the primary key in the data insertion state.
     *
     * @param compiledCondition     the compiledCondition against which records should be matched
     * @param conditionParameterMap the compiledCondition against which records should be matched
     */
    private List<String> findAllIDs(CompiledCondition compiledCondition,
                                    Map<String, Object> conditionParameterMap) {
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = cassandraStoreConfig.getRecordSelectNoKeyTable().
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName).
                replace(PLACEHOLDER_CONDITION, compiledQuery);
        PreparedStatement preparedStatement = session.prepare(finalSearchQuery);
        Object[] argSet = constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = preparedStatement.bind(argSet);
        List<String> ids = new ArrayList<>();
        ResultSet result = session.execute(bound);
        result.forEach(row -> ids.add(row.getString(CQL_ID)));
        return ids;
    }

    /**
     * This used to find all ids (Primary Key) to update or delete a row in case where
     * user has defined the primary key in the the condition.
     *
     * @param compiledCondition     the compiledCondition against which records should be matched
     * @param conditionParameterMap the compiledCondition against which records should be matched
     */
    private List<Object[]> findAllUserDefinedIDs(CompiledCondition compiledCondition,
                                                 Map<String, Object> conditionParameterMap) {
        // constructs the values to be extracted fro the relevent table
        // eg - select val1,val2,val3
        StringBuilder keyValueSelector = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> keyColumn : persistedKeyColumns.entrySet()) {
            keyValueSelector.append(keyColumn.getKey());
            if (i != (persistedKeyColumns.size() - 1)) {
                keyValueSelector.append(SEPARATOR);
            }
            i++;
        }

        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = cassandraStoreConfig.getRecordSelectQuery().
                replace(PLACEHOLDER_SELECT_VALUES, keyValueSelector.toString()).
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName).
                replace(PLACEHOLDER_CONDITION, compiledQuery);
        PreparedStatement preparedStatement = session.prepare(finalSearchQuery);
        Object[] argSet = constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = preparedStatement.bind(argSet);
        List<Object[]> ids = new ArrayList<>();
        ResultSet result = session.execute(bound);

        for (Row row : result) {
            Object[] rowKey = new Object[persistedKeyColumns.size()];
            int rowNo = 0;
            for (Map.Entry<String, String> persistedColumn : persistedKeyColumns.entrySet()) {
                rowKey[rowNo++] = row.getString(persistedColumn.getKey());
            }
            ids.add(rowKey);
        }
        return ids;
    }

    /**
     * This creates the prepared statement that is used in a table where a
     * primary key is not defined by the user as a batch and also the batch is executed.
     *
     * @param ids set of ids found to perform the operation
     */
    private String buildQueryForNoIdDelete(List<String> ids) {
        String deleteQuery = cassandraStoreConfig.getRecordDeleteQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).
                replace(PLACEHOLDER_TABLE, tableName).replace(PLACEHOLDER_CONDITION, CONDITION_CQL_ID);
        executeAsBatchNoIdDelete(ids, deleteQuery);
        return deleteQuery;
    }

    /**
     * This creates the prepared statement that is used in a table where a
     * primary key is not defined by the user as a batch and also the batch is executed.
     *
     * @param ids         set of ids found to perform the operation
     * @param deleteQuery delete query String
     */
    private void executeAsBatchNoIdDelete(List<String> ids, String deleteQuery) {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = session.prepare(deleteQuery);
        ids.forEach(id -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(id);
            batchStatement.add(boundStatement);
        });
        session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement (to delete the table) that is used in a table where a
     * primary key is not defined at the condition by the user as a batch and also the batch is executed.
     *
     * @param ids set of ids found to perform the operation
     */
    private void nonPrimaryKeyDelete(List<Object[]> ids) {
        int i = 0;
        StringBuilder condition = new StringBuilder();
        // building the condition statement
        for (Map.Entry<String, String> keyColumn : persistedKeyColumns.entrySet()) {
            condition.append(keyColumn.getKey()).append(CQL_EQUALS).append(QUESTION_MARK);
            if (i != (persistedKeyColumns.size() - 1)) {
                condition.append(WHITESPACE).append(CQL_AND).append(WHITESPACE);
            }
            i++;
        }
        String deleteQuery = cassandraStoreConfig.getRecordDeleteQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName).
                replace(PLACEHOLDER_CONDITION, condition.toString());
        executeAsBatchNonPrimaryKeyDelete(ids, deleteQuery);
    }

    /**
     * Executes the the delete operations in non primary key situation where the user does not define the primary keys.
     *
     * @param ids         List of object arrays which contains the ids
     * @param deleteQuery delete query string
     */
    private void executeAsBatchNonPrimaryKeyDelete(List<Object[]> ids, String deleteQuery) {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = session.prepare(deleteQuery);
        ids.forEach(id -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(id);
            batchStatement.add(boundStatement);
        });
        session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement (to update the table) that is used in a table where a
     * primary key (row key) is not defined at the condition by the user as a batch and also the batch is executed.
     *
     * @param ids                set of ids found to perform the operation
     * @param updateParameterMap set parameters used to update the table
     */
    private void executeAsBatchNonPrimaryKeyUpdate(List<Object[]> ids, Map<String, Object> updateParameterMap) {
        // keys that are in the condition as well as the set parameters
        // these should be removed
        List<String> keys = new ArrayList<>();
        updateParameterMap.forEach((parameter, value) -> {
            if (persistedKeyColumns.containsKey(parameter)) {
                keys.add(parameter);
            }
        });
        //Since cassandra cannot update a primary key column we need to remove the primary key values sent
        for (String key : keys) {
            if (updateParameterMap.containsKey(key)) {
                LOG.warn("Primary key " + key + " is included in the set values. Note that those values " +
                        "are ignored in the execution in " + keyspace + "." + tableName);
                updateParameterMap.remove(key);
            }
        }
        BatchStatement batchStatement = new BatchStatement();
        List<Object> setValues = buildUpdateStatement(updateParameterMap);
        PreparedStatement preparedStatement = session.prepare(updateQuery);

        ids.forEach(rowKey -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            List<Object> recordCells = new ArrayList<>();
            recordCells.addAll(setValues);
            recordCells.addAll(new ArrayList<>(Arrays.asList(rowKey)));
            boundStatement.bind(recordCells.toArray());
            batchStatement.add(boundStatement);
        });
        session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement that is used in a table where a
     * primary key is not defined by the user as a batch and also the batch is executed.
     *
     * @param ids                set of ids found to perform the operation
     * @param updateParameterMap set parameters used to update the table
     */
    private void executeAsBatchNoIdUpdate(List<String> ids, Map<String, Object> updateParameterMap) {
        BatchStatement batchStatement = new BatchStatement();
        List<Object> setValues = buildCQLUpdateSetStatement(updateParameterMap);
        String finalUpdateQuery = updateQuery.replace(PLACEHOLDER_CONDITION, CONDITION_CQL_ID);
        PreparedStatement preparedStatement = session.prepare(finalUpdateQuery);
        ids.forEach(rowKey -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            List<Object> recordCells = new ArrayList<>();
            recordCells.addAll(setValues);
            recordCells.add(rowKey);
            boundStatement.bind(recordCells.toArray());
            batchStatement.add(boundStatement);
        });
        session.execute(batchStatement);
    }

    /**
     * This is used to delete a single row entry that is matched with the primary key
     *
     * @param deleteConditionParameterMap set parameters used to delete a row the table
     * @param compiledCondition           the compiledCondition against which records should be matched
     */
    private void deleteSingleRow(Map<String, Object> deleteConditionParameterMap,
                                 CompiledCondition compiledCondition) {
        String deleteQuery = cassandraStoreConfig.getRecordDeleteQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).
                replace(PLACEHOLDER_TABLE, tableName).replace(PLACEHOLDER_CONDITION,
                ((CassandraCompiledCondition) compiledCondition).getCompiledQuery());
        Object[] argSet = constructArgSet(compiledCondition, deleteConditionParameterMap);
        PreparedStatement preparedStatement = session.prepare(deleteQuery);
        BoundStatement bound = preparedStatement.bind(argSet);
        session.execute(bound);
    }

    /**
     * This used to update a single row when the exact row key is provided
     *
     * @param updateSetParameterMap        set parameters used to update the table
     * @param compiledCondition            the compiledCondition against which records should be matched
     * @param updateConditionParameterMaps map of condition parameters
     */
    private void updateSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition,
                                 Map<String, Object> updateConditionParameterMaps) {
        List<String> keys = new ArrayList<>();
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();

        updateSetParameterMap.forEach((parameter, value) -> {
            if (persistedKeyColumns.containsKey(parameter)) {
                keys.add(parameter);
            }
        });
        //Since cassandra cannot update a primary key column we need to remove the primary key values sent
        for (String key : keys) {
            if (updateSetParameterMap.containsKey(key)) {
                LOG.warn("Primary key " + key + " is included in the set values. Note that those values " +
                        "are ignored in the execution in " + keyspace + "." + tableName);
                updateSetParameterMap.remove(key);
            }
        }
        List<Object> allValues = buildCQLUpdateSetStatement(updateSetParameterMap);
        String finalUpdateQuery = updateQuery.replace(PLACEHOLDER_CONDITION, compiledQuery);
        Object[] argSet = constructArgSet(compiledCondition, updateConditionParameterMaps);
        allValues.addAll(new ArrayList<>(Arrays.asList(argSet)));
        PreparedStatement preparedStatement = session.prepare(finalUpdateQuery);
        BoundStatement bound = preparedStatement.bind(allValues.toArray());
        session.execute(bound);
    }

    /**
     * This used to construct the CQL query to a given set parameters
     *
     * @param updateParameterMap set parameters used to update the table
     */
    private List<Object> buildCQLUpdateSetStatement(Map<String, Object> updateParameterMap) {
        String updateCql = cassandraStoreConfig.getRecordUpdateQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName);
        List<Object> setValues = new ArrayList<>();
        String updateParameterString = buildUpdateParameterValuesAndSetValues(updateParameterMap, setValues);
        updateQuery = updateCql.replace(PLACEHOLDER_COLUMNS_AND_VALUES, updateParameterString);
        return setValues;
    }

    /**
     * This used to update a single row when the exact row key is provided
     *
     * @param updateParameterMap set parameters used to update the table
     * @param setValues          a list of set values that is used to construct the update query
     * @return returns the update parameter string
     */
    private String buildUpdateParameterValuesAndSetValues(Map<String, Object> updateParameterMap,
                                                          List<Object> setValues) {
        int i = 1;
        StringBuilder updateParameterString = new StringBuilder();
        int size = updateParameterMap.size();
        for (Map.Entry<String, Object> parameter : updateParameterMap.entrySet()) {
            updateParameterString.append(parameter.getKey()).append(CQL_EQUALS).append(QUESTION_MARK);
            setValues.add(parameter.getValue());
            if (i != size) {
                updateParameterString.append(SEPARATOR);
            }
            i++;
        }
        return updateParameterString.toString();
    }

    /**
     * This is used to build the update set statement
     *
     * @param updateParameterMap set parameters used to update the table
     */
    private List<Object> buildUpdateStatement(Map<String, Object> updateParameterMap) {
        // builds the update statement with set values
        List<Object> setValues = buildCQLUpdateSetStatement(updateParameterMap);
        StringBuilder condition = new StringBuilder();
        int size = persistedKeyColumns.size();
        int i = 1;
        // builds the condition
        for (Map.Entry<String, String> column : persistedKeyColumns.entrySet()) {
            condition.append(column.getKey()).append(CQL_EQUALS).append(QUESTION_MARK);
            if (i != size) {
                condition.append(WHITESPACE).append(CQL_AND).append(WHITESPACE);
            }
            i++;
        }
        updateQuery = updateQuery.replace(PLACEHOLDER_CONDITION, condition);
        return setValues;
    }

    /**
     * This is used to updateOrAdd a row in table where primary key is not defined
     *
     * @param updateSetParameterMap set parameters used to update the table
     */
    private void updateOrAddToNoKeyTable(Map<String, Object> updateSetParameterMap) {
        List<Object> setValues = new ArrayList<>();
        String updateParameterString = buildUpdateParameterValuesAndSetValues(updateSetParameterMap, setValues);
        String finalUpdateQuery = cassandraStoreConfig.getRecordUpdateQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName).
                replace(PLACEHOLDER_COLUMNS_AND_VALUES, updateParameterString).
                replace(PLACEHOLDER_CONDITION, CONDITION_CQL_ID);
        setValues.add(CassandraTableUtils.generatePrimaryKeyValue());
        PreparedStatement preparedStatement = session.prepare(finalUpdateQuery);
        BoundStatement bound = preparedStatement.bind(setValues.toArray());
        session.execute(bound);
    }

    /**
     * This will call updateSingleRow (updating logic) used in update method
     *
     * @param updateSetParameterMap        set parameters used to update the table
     * @param compiledCondition            the compiledCondition against which records should be matched
     * @param updateConditionParameterMaps map of condition parameters
     */
    private void updateOrAddSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition,
                                      Map<String, Object> updateConditionParameterMaps) {
        updateSingleRow(updateSetParameterMap, compiledCondition, updateConditionParameterMaps);
    }

    /**
     * This will check whether a certain condition contains all the primary keys in the table
     *
     * @param paramList condition map
     * @return returns true if the condition condition contains all the primary keys
     */
    private boolean containsAllPrimaryKeys(Map<String, Object> paramList) {
        // null checking
        List<String> paramKeys = new ArrayList<>(paramList.keySet());

        if ((persistedKeyColumns == null) || (paramKeys.size() != persistedKeyColumns.size())) {
            return false;
        }
        for (Map.Entry<String, String> persistedColumn : persistedKeyColumns.entrySet()) {
            if (!paramKeys.contains(persistedColumn.getKey())) {
                return false;
            }
        }
        return true;
    }

    /**
     * This will check whether a certain condition contains all the primary keys in the table
     *
     * @param storeVariableNames condition map
     * @return returns true if the condition condition contains all the primary keys
     */
    private boolean containsAllPrimaryKeys(List<String> storeVariableNames) {
        // null checking
        if ((persistedKeyColumns == null) || (storeVariableNames.size() != persistedKeyColumns.size())) {
            return false;
        }
        for (String storeAttributeName : storeVariableNames) {
            if (!persistedKeyColumns.containsKey(storeAttributeName.toLowerCase(Locale.ENGLISH))) {
                return false;
            }
        }
        return true;
    }

    /**
     * This function is used in generating the value statement and the question marks to be used in prepared
     * statement and detecting object attributes
     */
    private void buildInsertAndSelectQuery() {
        int i = 0;
        StringBuilder insertValStatement = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();
        // keeping the indexes of object attributes
        objectAttributes = new ArrayList<>();
        for (Attribute a : schema) {
            insertValStatement.append(a.getName());
            questionMarks.append(QUESTION_MARK);
            //building the insert value statement
            if (i != schema.size() - 1) {
                insertValStatement.append(SEPARATOR);
                questionMarks.append(SEPARATOR);
            }
            //keeping the object attribute indexes in a separate array
            if (schema.get(i).getType() == Attribute.Type.OBJECT) {
                objectAttributes.add(i);
            }
            i++;
        }
        //initialising the select value statement
        String selectValStatement = insertValStatement.toString();
        if (noKeys) {
            questionMarks.append(SEPARATOR).append(QUESTION_MARK);
            insertValStatement.append(SEPARATOR).append(CQL_ID);
        }
        //query initialization to add data to the cassandra keyspace
        addDataQuerySt = cassandraStoreConfig.getRecordInsertQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).
                replace(PLACEHOLDER_TABLE, tableName).replace(PLACEHOLDER_INSERT_VALUES, insertValStatement).
                replace(PLACEHOLDER_QUESTION_MARKS, questionMarks);
        //Query to search data
        selectQuery = cassandraStoreConfig.getRecordSelectQuery().
                replace(PLACEHOLDER_SELECT_VALUES, selectValStatement).
                replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName);
    }

    /**
     * This will check whether the table is created if not will create the table
     */
    private void checkTable() {
        String checkStatement = cassandraStoreConfig.getTableCheckQuery().replace(PLACEHOLDER_KEYSPACE,
                keyspace.toLowerCase(Locale.ENGLISH)).replace(PLACEHOLDER_TABLE,
                tableName.toLowerCase(Locale.ENGLISH));
        ResultSet result;
        try {
            session.execute(checkStatement);
            createTable();
        } catch (AlreadyExistsException e) {
            if (!isTableWithDefinedColumns()) {
                throw new CassandraTableException("Problem with the table definition or key. " +
                        "Please re check the table schema and try again.");
            }
            LOG.debug("Table " + tableName + " is exists in database " + keyspace);
        }

    }

    /**
     * This will check whether the table is created if not will create the table
     */
    private void createTable() {
        StringBuilder primaryKeyStatement = new StringBuilder();
        primaryKeyStatement.append(CQL_PRIMARY_KEY_DEF).append(OPEN_PARENTHESIS);
        int i = 0;
        for (Attribute primaryKey : primaryKeys) {
            primaryKeyStatement.append(primaryKey.getName());
            if (i != primaryKeys.size() - 1) {
                primaryKeyStatement.append(SEPARATOR);
            } else {
                primaryKeyStatement.append(CLOSE_PARENTHESIS);
            }
            i++;
        }

        StringBuilder attributeStatement = new StringBuilder();
        schema.forEach(attribute -> {
            attributeStatement.append(attribute.getName());
            attributeStatement.append(WHITESPACE);
            String type = CassandraTableUtils.convertToCassandraDataTypes(attribute.getType());
            attributeStatement.append(type);
            attributeStatement.append(SEPARATOR);
        });
        //when primary key is not given
        if (noKeys) {
            attributeStatement.append(CQL_ID).append(WHITESPACE).append(CQL_TEXT).append(WHITESPACE).append(SEPARATOR);
            noKeyTable = true;
        }

        String createStatement = cassandraStoreConfig.getTableCreateQuery().
                replace(PLACEHOLDER_KEYSPACE, keyspace).
                replace(PLACEHOLDER_TABLE, tableName).replace(PLACEHOLDER_COLUMNS, attributeStatement).
                replace(PLACEHOLDER_PRIMARY_KEYS, primaryKeyStatement);
        session.execute(createStatement);
        if (indexAnnotation != null) {
            initIndexQuery();
        }
        findPersistedKeys();
    }

    /**
     * User defined keys that are actually defined in the keyspace
     */
    private void findPersistedKeys() {
        String checkStatement = cassandraStoreConfig.getTableValidityQuery().replace(PLACEHOLDER_KEYSPACE,
                keyspace.toLowerCase(Locale.ENGLISH)).
                replace(PLACEHOLDER_TABLE, tableName.toLowerCase(Locale.ENGLISH));
        ResultSet result = session.execute(checkStatement);
        List<TableMeta> tableColumns = new ArrayList<>();

        for (Row row : result) {
            TableMeta tableMeta = new TableMeta(row.getString(TABLE_PROPERTY_COLUMN_NAME),
                    row.getString(TABLE_PROPERTY_KIND),
                    row.getString(TABLE_PROPERTY_TYPE));
            tableColumns.add(tableMeta);
        }

        persistedKeyColumns = new HashMap<>();
        tableColumns.stream().filter(column -> column.getKeyType().equals(TABLE_PROPERTY_PARTITION_KEY) ||
                column.getKeyType().equals(TABLE_PROPERTY_CLUSTERING_KEY))
                .forEach(column -> persistedKeyColumns.put(column.getColumnName(), column.getDataType()));
    }

    /**
     * This will check whether the table and keyspace is valid before creating the table
     */
    private boolean isValidKeyspaceAndTable() {
        String pattern = "^[A-Za-z0-9_]*$";
        Pattern r = Pattern.compile(pattern);
        Matcher kp = r.matcher(keyspace);
        Matcher table = r.matcher(tableName);
        return (kp.find() && table.find());
    }


    /**
     * This will check whether defined column already exists in the table and the defined primary keys
     * are already as the previously defined ones
     */
    private boolean isTableWithDefinedColumns() {
//        String checkStatement = cassandraStoreConfig.getTableValidityQuery().replace(PLACEHOLDER_KEYSPACE,
//                keyspace.toLowerCase(Locale.ENGLISH)).
//                replace(PLACEHOLDER_TABLE, tableName.toLowerCase(Locale.ENGLISH));
//        ResultSet result = session.execute(checkStatement);
//        Map<String, String> tableDet = new HashMap<>();
//        List<TableMeta> tableColumns = new ArrayList<>();
//
//        result.forEach(row -> {
//            tableDet.put(row.getString(TABLE_PROPERTY_COLUMN_NAME), row.getString(TABLE_PROPERTY_KIND));
//            TableMeta tableMeta = new TableMeta(row.getString(TABLE_PROPERTY_COLUMN_NAME),
//                    row.getString(TABLE_PROPERTY_KIND), row.getString(TABLE_PROPERTY_TYPE));
//            tableColumns.add(tableMeta);
//        });
//
//        persistedKeyColumns = new HashMap<>();
//        Map<String, String> persistedColumns = initPersistedColumns(tableColumns);
        return true;
    }

    /**
     * This method will initialize the persisted keys of the actual table in the keyspace
     *
     * @param tableColumns columns in the table
     * @return Map of persisted columns with their corresponding data type
     */
    private Map<String, String> initPersistedColumns(List<TableMeta> tableColumns) {
        persistedKeyColumns = new HashMap<>();
        Map<String, String> persistedColumns = new HashMap<>();

        tableColumns.forEach(column -> {
            if (column.getKeyType().equals(TABLE_PROPERTY_PARTITION_KEY) || column.getKeyType().
                    equals(TABLE_PROPERTY_CLUSTERING_KEY)) {
                persistedKeyColumns.put(column.getColumnName(), column.getDataType());
            }
            persistedColumns.put(column.getColumnName(), column.getDataType());
        });
        return persistedColumns;
    }

    /**
     * This method checks the validity of keys and their relevent data type
     *
     * @param tableDet         a map which contains the details of the table
     * @param persistedColumns a map which contains the details about the persisted columns
     * @return validity of the keys and the relevent data types
     */
    private boolean checkKeyAndDataTypeValidity(Map<String, String> tableDet, Map<String, String> persistedColumns) {
        boolean validColumns;
        boolean validDataTypes;
        // To Check whether the column names match with the persisted column names
        for (Attribute attribute : schema) {
            validColumns = tableDet.containsKey(attribute.getName().toLowerCase(Locale.ENGLISH));
            if (!validColumns) {
                return false;
            }
        }
        // To Check whether the column data types match with the persisted column data types names
        for (Attribute attribute : schema) {
            String persistedColumnType = persistedColumns.get(attribute.getName().toLowerCase(Locale.ENGLISH));
            String inComingDataType = CassandraTableUtils.convertToCassandraDataTypes(attribute.getType());
            validDataTypes = persistedColumnType.equalsIgnoreCase(inComingDataType);
            if (!validDataTypes) {
                return false;
            }
        }
        return isValidKeys(tableDet);
    }

    /**
     * This method is used to check whether the keys are valid
     *
     * @param tableDet a map which contains the details of the table
     * @return returns the validity of the keys
     */
    private boolean isValidKeys(Map<String, String> tableDet) {
        boolean validKeys = false;
        if (noKeys) {
            validKeys = tableDet.containsKey("_id");
            noKeyTable = validKeys;
        } else {
            for (Attribute attribute : primaryKeys) {
                String pk = attribute.getName().toLowerCase(Locale.ENGLISH);
                validKeys = (tableDet.containsKey(pk) &&
                        (tableDet.get(pk).equals(TABLE_PROPERTY_PARTITION_KEY) || tableDet.get(pk).
                                equals(TABLE_PROPERTY_CLUSTERING_KEY)));
                if (!validKeys) {
                    return false;
                }
            }
        }
        return validKeys;
    }

    /**
     * This check the indexAnnotation and create indexes on the given columns
     */
    private void initIndexQuery() {
        String[] derivedIndexes = indexAnnotation.getElements().get(0).getValue().split(SEPARATOR);
        this.indexes = new ArrayList<>();
        Arrays.stream(derivedIndexes).forEach(index ->
                schema.stream()
                        .filter(attribute -> attribute.getName().trim().equals(index))
                        .forEach(attribute -> {
                            String indexQuery = cassandraStoreConfig.getIndexQuery().
                                    replace(PLACEHOLDER_KEYSPACE, keyspace).replace(PLACEHOLDER_TABLE, tableName).
                                    replace(PLACEHOLDER_INDEX, index);
                            session.execute(indexQuery);
                            this.indexes.add(index);
                        })
        );
    }
}
