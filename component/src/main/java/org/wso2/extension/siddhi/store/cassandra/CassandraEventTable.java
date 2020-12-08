//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

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
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.Attribute.Type;
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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

@Extension(
        name = "cassandra",
        namespace = "store",
        description = "This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected datasource.",
        parameters = {@Parameter(
                name = "cassandra.host",
                description = "Host that is used to get connected in to the cassandra keyspace.",
                type = {DataType.STRING},
                defaultValue = "localhost"
        ), @Parameter(
                name = "column.family",
                description = "The name with which the event table should be persisted in the store. If no name is specified via this parameter, the event table is persisted with the same name as the Siddhi table.",
                type = {DataType.STRING},
                optional = true,
                defaultValue = "The table name defined in the Siddhi Application query."
        ), @Parameter(
                name = "client.port",
                description = "Client port that is used to get connected with the client store. If no name is specified via this parameter, the default port is taken.",
                type = {DataType.STRING},
                optional = true,
                defaultValue = "9042"
        ), @Parameter(
                name = "keyspace",
                description = "User need to give the keyspace that the data is persisted. Keyspace name is specified via this parameter.",
                type = {DataType.STRING},
                defaultValue = "cassandraTestTable"
        ), @Parameter(
                name = "username",
                description = "Through user name user can specify the relevent username that is used to log in to the cassandra keyspace .",
                type = {DataType.STRING},
                optional = true,
                defaultValue = "The username of the keyspace"
        ), @Parameter(
                name = "password",
                description = "Through password user can specify the relevent password that is used to log in to the cassandra keyspace .",
                type = {DataType.STRING},
                optional = true,
                defaultValue = "The password of the keyspace"
        )},
        examples = {@Example(
                syntax = "define stream StockStream (symbol string, price float, volume long); \n@Store(type='cassandra', column.family='StockTable',keyspace='AnalyticsFamily',username='cassandra',password='cassandra',cassandra.host='localhost')@IndexBy('volume')@PrimaryKey('symbol')define table StockTable (symbol string, price float, volume long); ",
                description = "This definition creates an event table named `StockTable` in the AnalyticsFamily keyspace on the Cassandra instance if it does not already exist (with 3 attributes named `symbol`, `price`, and `volume` of the `string`, `float` and `long` types respectively). The connection is made as specified by the parameters configured for the '@Store' annotation. The `symbol` attribute is considered a unique field, and the values for this attribute are the Cassandra row IDs."
        )}
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
    private List<Integer> objectAttributes;
    private boolean noKeys;
    private String selectQuery;
    private String updateQuery;
    private Annotation indexAnnotation;
    private boolean noKeyTable;
    private Map<String, String> persistedKeyColumns;
    private CassandraStoreConfig cassandraStoreConfig;
    private List<String> indexes;
    private int port;

    public CassandraEventTable() {
    }

    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.schema = tableDefinition.getAttributeList();
        this.storeAnnotation = AnnotationHelper.getAnnotation("Store", tableDefinition.getAnnotations());
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation("PrimaryKey", tableDefinition.getAnnotations());
        this.indexAnnotation = AnnotationHelper.getAnnotation("IndexBy", tableDefinition.getAnnotations());
        String storeAnnotationTableName = this.storeAnnotation.getElement("column.family");
        String storeAnnotationKeyspace = this.storeAnnotation.getElement("keyspace");
        String storeAnnotationPort = this.storeAnnotation.getElement("client.port");
        this.tableName = CassandraTableUtils.isEmpty(storeAnnotationTableName) ? tableDefinition.getId() : storeAnnotationTableName;
        this.host = this.storeAnnotation.getElement("cassandra.host");
        this.keyspace = CassandraTableUtils.isEmpty(storeAnnotationKeyspace) ? "wso2sp" : storeAnnotationKeyspace;
        this.port = CassandraTableUtils.isEmpty(storeAnnotationPort) ? 9042 : Integer.parseInt(storeAnnotationPort);

        try {
            this.cassandraStoreConfig = (new CassandraTableUtils()).readConfigFile(configReader);
        } catch (CannotLoadConfigurationException var8) {
            throw new CassandraTableException("Cannot find a cassandra configuration for the keyspace " + this.keyspace + var8.getMessage(), var8);
        }

        if (primaryKeyAnnotation == null) {
            this.noKeys = true;
            this.primaryKeys = new ArrayList();
            Attribute primaryKey = new Attribute("\"_id\"", Type.STRING);
            this.primaryKeys.add(primaryKey);
        } else {
            this.primaryKeys = CassandraTableUtils.initPrimaryKeys(this.schema, primaryKeyAnnotation);
        }
        this.buildInsertAndSelectQuery();
    }

    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        PreparedStatement preparedStatement = this.session.prepare(this.addDataQuerySt);

        Object[] record;
        for (Iterator var3 = records.iterator(); var3.hasNext(); this.addData(record, preparedStatement)) {
            record = (Object[]) var3.next();
            if (!this.objectAttributes.isEmpty()) {
                Object[] finalRecord = record;
                this.objectAttributes.forEach((columnNo) -> {
                    Object oldData = finalRecord[columnNo];

                    try {
                        finalRecord[columnNo] = this.resolveObjectData(oldData);
                    } catch (IOException var5) {
                        throw new CassandraTableException("Error in object insertion ensure that the objects are serializable.", var5);
                    }
                });
            }
        }

    }

    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        String compiledQuery = cassandraCompiledCondition.getCompiledQuery();
        String finalSearchQuery;
        ResultSet result;
        if (compiledQuery.isEmpty()) {
            finalSearchQuery = this.selectQuery.replace("{{CONDITION}}", "").replace("WHERE", "").replace("ALLOW FILTERING", "");
            result = this.session.execute(finalSearchQuery);
        } else {
            List<String> storeAttributeNames = cassandraCompiledCondition.getStoreAttributeNames();
            boolean containsAllPrimaryKeys = this.containsAllPrimaryKeys(storeAttributeNames);
            PreparedStatement preparedStatement;
            Object[] argSet;
            BoundStatement bound;
            if (!cassandraCompiledCondition.getReadOnlyCondition() || (!this.containsAllPrimaryKeys(storeAttributeNames) || !this.queryContainsOnlyOneIndex(storeAttributeNames)) && !containsAllPrimaryKeys) {
                finalSearchQuery = this.selectQuery.replace("{{CONDITION}}", compiledQuery);
                preparedStatement = this.session.prepare(finalSearchQuery);
                argSet = this.constructArgSet(compiledCondition, findConditionParameterMap);
                bound = preparedStatement.bind(argSet);
                result = this.session.execute(bound);
            } else {
                finalSearchQuery = this.selectQuery.replace("{{CONDITION}}", compiledQuery).replace("ALLOW FILTERING", "");
                preparedStatement = this.session.prepare(finalSearchQuery);
                argSet = this.constructArgSet(compiledCondition, findConditionParameterMap);
                bound = preparedStatement.bind(argSet);
                result = this.session.execute(bound);
            }
        }

        return new CassandraIterator(result.iterator(), this.schema);
    }

    private boolean queryContainsOnlyOneIndex(List<String> storeAttributeNames) {
        if (this.indexes == null) {
            return false;
        } else {
            int noOfIndexesInFind = 0;
            Iterator var3 = this.indexes.iterator();

            do {
                if (!var3.hasNext()) {
                    return noOfIndexesInFind == 1;
                }

                String index = (String) var3.next();
                if (storeAttributeNames.contains(index)) {
                    ++noOfIndexesInFind;
                }
            } while (noOfIndexesInFind != 2);

            return false;
        }
    }

    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        Object[] argSet = this.constructArgSet(compiledCondition, containsConditionParameterMap);
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String cql = this.cassandraStoreConfig.getRecordExistQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", compiledQuery);
        PreparedStatement preparedStatement = this.session.prepare(cql);
        BoundStatement boundStatement = preparedStatement.bind(argSet);
        ResultSet rs = this.session.execute(boundStatement);
        return rs.one() != null;
    }

    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps, Map<String, CompiledExpression> updateSetExpressions, List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        int i = 0;

        for (Iterator var6 = updateSetParameterMaps.iterator(); var6.hasNext(); ++i) {
            Map<String, Object> updateSetParameterMap = (Map) var6.next();
            if (!this.noKeyTable && this.containsAllPrimaryKeys((Map) updateConditionParameterMaps.get(i)) && ((CassandraCompiledCondition) updateCondition).getReadOnlyCondition()) {
                if (!this.contains((Map) updateConditionParameterMaps.get(i), updateCondition)) {
                    throw new CassandraTableException("Row does not exist with the provided keys.. Try to update with existing keys. Update failed. ");
                }

                this.updateSingleRow(updateSetParameterMap, updateCondition, (Map) updateConditionParameterMaps.get(i));
            } else {
                List ids;
                if (this.noKeyTable) {
                    ids = this.findAllIDs(updateCondition, (Map) updateConditionParameterMaps.get(i));
                    this.executeAsBatchNoIdUpdate(ids, updateSetParameterMap);
                } else {
                    ids = this.findAllUserDefinedIDs(updateCondition, (Map) updateConditionParameterMaps.get(i));
                    this.executeAsBatchNonPrimaryKeyUpdate(ids, updateSetParameterMap);
                }
            }
        }

    }

    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        deleteConditionParameterMaps.forEach((deleteConditionMap) -> {
            if (!this.noKeyTable && cassandraCompiledCondition.getReadOnlyCondition() && this.containsAllPrimaryKeys(cassandraCompiledCondition.getStoreAttributeNames())) {
                this.deleteSingleRow(deleteConditionMap, compiledCondition);
            } else {
                List ids;
                if (this.noKeyTable) {
                    ids = this.findAllIDs(compiledCondition, deleteConditionMap);
                    String deleteQuery = this.buildQueryForNoIdDelete(ids);
                    this.executeAsBatchNoIdDelete(ids, deleteQuery);
                } else {
                    ids = this.findAllUserDefinedIDs(compiledCondition, deleteConditionMap);
                    this.nonPrimaryKeyDelete(ids);
                }
            }

        });
    }

    protected void updateOrAdd(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps, Map<String, CompiledExpression> updateSetExpressions, List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords) throws ConnectionUnavailableException {
        int i = 0;
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) updateCondition;
        Iterator var8 = updateSetParameterMaps.iterator();

        while (true) {
            while (var8.hasNext()) {
                Map<String, Object> updateSetParameterMap = (Map) var8.next();
                if (!this.noKeyTable && cassandraCompiledCondition.getReadOnlyCondition() && this.containsAllPrimaryKeys(cassandraCompiledCondition.getStoreAttributeNames())) {
                    this.updateOrAddSingleRow(updateSetParameterMap, updateCondition, (Map) updateConditionParameterMaps.get(i));
                } else {
                    List ids;
                    if (this.noKeyTable) {
                        ids = this.findAllIDs(updateCondition, (Map) updateConditionParameterMaps.get(i));
                        if (ids.isEmpty()) {
                            this.updateOrAddToNoKeyTable(updateSetParameterMap);
                        } else {
                            this.executeAsBatchNoIdUpdate(ids, updateSetParameterMap);
                        }
                    } else {
                        ids = this.findAllUserDefinedIDs(updateCondition, (Map) updateConditionParameterMaps.get(i));
                        if (ids.isEmpty()) {
                            throw new CassandraTableException("No results found for the given values. Only update functionality is capable without primary keys. If update or insert operation is neededwhole key should be included");
                        }

                        this.executeAsBatchNonPrimaryKeyUpdate(ids, updateSetParameterMap);
                        ++i;
                    }
                }
            }

            return;
        }
    }

    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CassandraConditionVisitor visitor = new CassandraConditionVisitor();
        expressionBuilder.build(visitor);
        return new CassandraCompiledCondition(visitor.returnCondition(), visitor.getParameters(), visitor.getReadOnlyCondition(), visitor.getStoreAtrributeNames());
    }

    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return this.compileCondition(expressionBuilder);
    }

    protected void connect() throws ConnectionUnavailableException {
        String username = this.storeAnnotation.getElement("username");
        String password = this.storeAnnotation.getElement("password");

        try {
            Cluster cluster;
            if (username.isEmpty() && password.isEmpty()) {
                cluster = Cluster.builder().addContactPoint(this.host).withPort(this.port).build();
            } else {
                cluster = Cluster.builder().addContactPoint(this.host).withPort(this.port).withCredentials(username, password).build();
            }

            this.session = cluster.connect(this.keyspace);
        } catch (NoHostAvailableException var5) {
            throw new ConnectionUnavailableException("Tried to connect to a host, but no host is found.", var5);
        }

        this.checkTable();
        LOG.info("Store " + this.keyspace + "." + this.tableName + " is initialized");
    }

    protected void disconnect() {
        if (this.session != null) {
            this.session.close();
        }

    }

    protected void destroy() {
    }

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

    private void addData(Object[] record, PreparedStatement preparedStatement) {
        BoundStatement bound;
        if (this.noKeyTable) {
            Object[] withPrimaryKey = Arrays.copyOf(record, record.length + 1);
            withPrimaryKey[record.length] = CassandraTableUtils.generatePrimaryKeyValue();
            bound = preparedStatement.bind(withPrimaryKey);
        } else {
            bound = preparedStatement.bind(record);
        }

        this.session.execute(bound);
    }

    private Object[] constructArgSet(CompiledCondition compiledCondition, Map<String, Object> conditionParameterMap) {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        SortedMap<Integer, Object> compiledParameters = cassandraCompiledCondition.getParameters();
        Object[] argSet = new Object[compiledParameters.size()];
        int i = 0;

        for (Iterator var7 = compiledParameters.entrySet().iterator(); var7.hasNext(); ++i) {
            Entry<Integer, Object> entry = (Entry) var7.next();
            Object parameter = entry.getValue();
            if (parameter instanceof Constant) {
                Constant constant = (Constant) parameter;
                argSet[i] = constant.getValue();
            } else {
                Attribute variable = (Attribute) parameter;
                String attributeName = variable.getName();
                argSet[i] = conditionParameterMap.get(attributeName);
            }
        }

        return argSet;
    }

    private List<String> findAllIDs(CompiledCondition compiledCondition, Map<String, Object> conditionParameterMap) {
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = this.cassandraStoreConfig.getRecordSelectNoKeyTable().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", compiledQuery);
        PreparedStatement preparedStatement = this.session.prepare(finalSearchQuery);
        Object[] argSet = this.constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = preparedStatement.bind(argSet);
        List<String> ids = new ArrayList();
        ResultSet result = this.session.execute(bound);
        result.forEach((row) -> {
            ids.add(row.getString("\"_id\""));
        });
        return ids;
    }

    private List<Object[]> findAllUserDefinedIDs(CompiledCondition compiledCondition, Map<String, Object> conditionParameterMap) {
        StringBuilder keyValueSelector = new StringBuilder();
        int i = 0;

        for (Iterator var5 = this.persistedKeyColumns.entrySet().iterator(); var5.hasNext(); ++i) {
            Entry<String, String> keyColumn = (Entry) var5.next();
            keyValueSelector.append((String) keyColumn.getKey());
            if (i != this.persistedKeyColumns.size() - 1) {
                keyValueSelector.append(",");
            }
        }

        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = this.cassandraStoreConfig.getRecordSelectQuery().replace("{{SELECT_VALUES}}", keyValueSelector.toString()).replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", compiledQuery);
        PreparedStatement preparedStatement = this.session.prepare(finalSearchQuery);
        Object[] argSet = this.constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = preparedStatement.bind(argSet);
        List<Object[]> ids = new ArrayList();
        ResultSet result = this.session.execute(bound);
        Iterator var12 = result.iterator();

        while (var12.hasNext()) {
            Row row = (Row) var12.next();
            Object[] rowKey = new Object[this.persistedKeyColumns.size()];
            int rowNo = 0;

            Entry persistedColumn;
            for (Iterator var16 = this.persistedKeyColumns.entrySet().iterator(); var16.hasNext(); rowKey[rowNo++] = row.getString((String) persistedColumn.getKey())) {
                persistedColumn = (Entry) var16.next();
            }

            ids.add(rowKey);
        }

        return ids;
    }

    private String buildQueryForNoIdDelete(List<String> ids) {
        String deleteQuery = this.cassandraStoreConfig.getRecordDeleteQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", "\"_id\"=?");
        this.executeAsBatchNoIdDelete(ids, deleteQuery);
        return deleteQuery;
    }

    private void executeAsBatchNoIdDelete(List<String> ids, String deleteQuery) {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = this.session.prepare(deleteQuery);
        ids.forEach((id) -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(new Object[]{id});
            batchStatement.add(boundStatement);
        });
        this.session.execute(batchStatement);
    }

    private void nonPrimaryKeyDelete(List<Object[]> ids) {
        int i = 0;
        StringBuilder condition = new StringBuilder();

        for (Iterator var4 = this.persistedKeyColumns.entrySet().iterator(); var4.hasNext(); ++i) {
            Entry<String, String> keyColumn = (Entry) var4.next();
            condition.append((String) keyColumn.getKey()).append("=").append("?");
            if (i != this.persistedKeyColumns.size() - 1) {
                condition.append(" ").append("AND").append(" ");
            }
        }

        String deleteQuery = this.cassandraStoreConfig.getRecordDeleteQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", condition.toString());
        this.executeAsBatchNonPrimaryKeyDelete(ids, deleteQuery);
    }

    private void executeAsBatchNonPrimaryKeyDelete(List<Object[]> ids, String deleteQuery) {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = this.session.prepare(deleteQuery);
        ids.forEach((id) -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(id);
            batchStatement.add(boundStatement);
        });
        this.session.execute(batchStatement);
    }

    private void executeAsBatchNonPrimaryKeyUpdate(List<Object[]> ids, Map<String, Object> updateParameterMap) {
        List<String> keys = new ArrayList();
        updateParameterMap.forEach((parameter, value) -> {
            if (this.persistedKeyColumns.containsKey(parameter)) {
                keys.add(parameter);
            }

        });
        Iterator var4 = keys.iterator();

        while (var4.hasNext()) {
            String key = (String) var4.next();
            if (updateParameterMap.containsKey(key)) {
                LOG.warn("Primary key " + key + " is included in the set values. Note that those values are ignored in the execution in " + this.keyspace + "." + this.tableName);
                updateParameterMap.remove(key);
            }
        }

        BatchStatement batchStatement = new BatchStatement();
        List<Object> setValues = this.buildUpdateStatement(updateParameterMap);
        PreparedStatement preparedStatement = this.session.prepare(this.updateQuery);
        ids.forEach((rowKey) -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            List<Object> recordCells = new ArrayList();
            recordCells.addAll(setValues);
            recordCells.addAll(new ArrayList(Arrays.asList(rowKey)));
            boundStatement.bind(recordCells.toArray());
            batchStatement.add(boundStatement);
        });
        this.session.execute(batchStatement);
    }

    private void executeAsBatchNoIdUpdate(List<String> ids, Map<String, Object> updateParameterMap) {
        BatchStatement batchStatement = new BatchStatement();
        List<Object> setValues = this.buildCQLUpdateSetStatement(updateParameterMap);
        String finalUpdateQuery = this.updateQuery.replace("{{CONDITION}}", "\"_id\"=?");
        PreparedStatement preparedStatement = this.session.prepare(finalUpdateQuery);
        ids.forEach((rowKey) -> {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            List<Object> recordCells = new ArrayList();
            recordCells.addAll(setValues);
            recordCells.add(rowKey);
            boundStatement.bind(recordCells.toArray());
            batchStatement.add(boundStatement);
        });
        this.session.execute(batchStatement);
    }

    private void deleteSingleRow(Map<String, Object> deleteConditionParameterMap, CompiledCondition compiledCondition) {
        String deleteQuery = this.cassandraStoreConfig.getRecordDeleteQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{CONDITION}}", ((CassandraCompiledCondition) compiledCondition).getCompiledQuery());
        Object[] argSet = this.constructArgSet(compiledCondition, deleteConditionParameterMap);
        PreparedStatement preparedStatement = this.session.prepare(deleteQuery);
        BoundStatement bound = preparedStatement.bind(argSet);
        this.session.execute(bound);
    }

    private void updateSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition, Map<String, Object> updateConditionParameterMaps) {
        List<String> keys = new ArrayList();
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        updateSetParameterMap.forEach((parameter, value) -> {
            if (this.persistedKeyColumns.containsKey(parameter)) {
                keys.add(parameter);
            }

        });
        Iterator var6 = keys.iterator();

        String key;
        while (var6.hasNext()) {
            key = (String) var6.next();
            if (updateSetParameterMap.containsKey(key)) {
                LOG.warn("Primary key " + key + " is included in the set values. Note that those values are ignored in the execution in " + this.keyspace + "." + this.tableName);
                updateSetParameterMap.remove(key);
            }
        }

        List<Object> allValues = this.buildCQLUpdateSetStatement(updateSetParameterMap);
        key = this.updateQuery.replace("{{CONDITION}}", compiledQuery);
        Object[] argSet = this.constructArgSet(compiledCondition, updateConditionParameterMaps);
        allValues.addAll(new ArrayList(Arrays.asList(argSet)));
        PreparedStatement preparedStatement = this.session.prepare(key);
        BoundStatement bound = preparedStatement.bind(allValues.toArray());
        this.session.execute(bound);
    }

    private List<Object> buildCQLUpdateSetStatement(Map<String, Object> updateParameterMap) {
        String updateCql = this.cassandraStoreConfig.getRecordUpdateQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName);
        List<Object> setValues = new ArrayList();
        String updateParameterString = this.buildUpdateParameterValuesAndSetValues(updateParameterMap, setValues);
        this.updateQuery = updateCql.replace("{{COLUMNS_AND_VALUES}}", updateParameterString);
        return setValues;
    }

    private String buildUpdateParameterValuesAndSetValues(Map<String, Object> updateParameterMap, List<Object> setValues) {
        int i = 1;
        StringBuilder updateParameterString = new StringBuilder();
        int size = updateParameterMap.size();

        for (Iterator var6 = updateParameterMap.entrySet().iterator(); var6.hasNext(); ++i) {
            Entry<String, Object> parameter = (Entry) var6.next();
            updateParameterString.append((String) parameter.getKey()).append("=").append("?");
            setValues.add(parameter.getValue());
            if (i != size) {
                updateParameterString.append(",");
            }
        }

        return updateParameterString.toString();
    }

    private List<Object> buildUpdateStatement(Map<String, Object> updateParameterMap) {
        List<Object> setValues = this.buildCQLUpdateSetStatement(updateParameterMap);
        StringBuilder condition = new StringBuilder();
        int size = this.persistedKeyColumns.size();
        int i = 1;

        for (Iterator var6 = this.persistedKeyColumns.entrySet().iterator(); var6.hasNext(); ++i) {
            Entry<String, String> column = (Entry) var6.next();
            condition.append((String) column.getKey()).append("=").append("?");
            if (i != size) {
                condition.append(" ").append("AND").append(" ");
            }
        }

        this.updateQuery = this.updateQuery.replace("{{CONDITION}}", condition);
        return setValues;
    }

    private void updateOrAddToNoKeyTable(Map<String, Object> updateSetParameterMap) {
        List<Object> setValues = new ArrayList();
        String updateParameterString = this.buildUpdateParameterValuesAndSetValues(updateSetParameterMap, setValues);
        String finalUpdateQuery = this.cassandraStoreConfig.getRecordUpdateQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{COLUMNS_AND_VALUES}}", updateParameterString).replace("{{CONDITION}}", "\"_id\"=?");
        setValues.add(CassandraTableUtils.generatePrimaryKeyValue());
        PreparedStatement preparedStatement = this.session.prepare(finalUpdateQuery);
        BoundStatement bound = preparedStatement.bind(setValues.toArray());
        this.session.execute(bound);
    }

    private void updateOrAddSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition, Map<String, Object> updateConditionParameterMaps) {
        this.updateSingleRow(updateSetParameterMap, compiledCondition, updateConditionParameterMaps);
    }

    private boolean containsAllPrimaryKeys(Map<String, Object> paramList) {
        List<String> paramKeys = new ArrayList(paramList.keySet());
        if (this.persistedKeyColumns != null && paramKeys.size() == this.persistedKeyColumns.size()) {
            Iterator var3 = this.persistedKeyColumns.entrySet().iterator();

            Entry persistedColumn;
            do {
                if (!var3.hasNext()) {
                    return true;
                }

                persistedColumn = (Entry) var3.next();
            } while (paramKeys.contains(persistedColumn.getKey()));

            return false;
        } else {
            return false;
        }
    }

    private boolean containsAllPrimaryKeys(List<String> storeVariableNames) {
        if (this.persistedKeyColumns != null && storeVariableNames.size() == this.persistedKeyColumns.size()) {
            Iterator var2 = storeVariableNames.iterator();

            String storeAttributeName;
            do {
                if (!var2.hasNext()) {
                    return true;
                }

                storeAttributeName = (String) var2.next();
            } while (this.persistedKeyColumns.containsKey(storeAttributeName.toLowerCase(Locale.ENGLISH)));

            return false;
        } else {
            return false;
        }
    }

    private void buildInsertAndSelectQuery() {
        int i = 0;
        StringBuilder insertValStatement = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();
        this.objectAttributes = new ArrayList();

        for (Iterator var4 = this.schema.iterator(); var4.hasNext(); ++i) {
            Attribute a = (Attribute) var4.next();
            insertValStatement.append(a.getName());
            questionMarks.append("?");
            if (i != this.schema.size() - 1) {
                insertValStatement.append(",");
                questionMarks.append(",");
            }

            if (((Attribute) this.schema.get(i)).getType() == Type.OBJECT) {
                this.objectAttributes.add(i);
            }
        }

        String selectValStatement = insertValStatement.toString();
        if (this.noKeys) {
            questionMarks.append(",").append("?");
            insertValStatement.append(",").append("\"_id\"");
        }

        this.addDataQuerySt = this.cassandraStoreConfig.getRecordInsertQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{INSERT_VALUES}}", insertValStatement).replace("{{QUESTION_MARKS}}", questionMarks);
        this.selectQuery = this.cassandraStoreConfig.getRecordSelectQuery().replace("{{SELECT_VALUES}}", selectValStatement).replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName);
    }

    private void checkTable() {
        String checkStatement = this.cassandraStoreConfig.getTableCheckQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName);

        try {
            this.session.execute(checkStatement);
            this.createTable();
        } catch (AlreadyExistsException var4) {
            LOG.debug("Table " + this.tableName + " is exists in database " + this.keyspace);
        }

    }

    private void createTable() {
        StringBuilder primaryKeyStatement = new StringBuilder();
        primaryKeyStatement.append("PRIMARY KEY").append("(");
        int i = 0;

        for (Iterator var3 = this.primaryKeys.iterator(); var3.hasNext(); ++i) {
            Attribute primaryKey = (Attribute) var3.next();
            primaryKeyStatement.append(primaryKey.getName());
            if (i != this.primaryKeys.size() - 1) {
                primaryKeyStatement.append(",");
            } else {
                primaryKeyStatement.append(")");
            }
        }

        StringBuilder attributeStatement = new StringBuilder();
        this.schema.forEach((attribute) -> {
            attributeStatement.append(attribute.getName());
            attributeStatement.append(" ");
            String type = CassandraTableUtils.convertToCassandraDataTypes(attribute.getType());
            attributeStatement.append(type);
            attributeStatement.append(",");
        });
        if (this.noKeys) {
            attributeStatement.append("\"_id\"").append(" ").append("text").append(" ").append(",");
            this.noKeyTable = true;
        }

        String createStatement = this.cassandraStoreConfig.getTableCreateQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{COLUMNS}}", attributeStatement).replace("{{PRIMARY_KEYS}}", primaryKeyStatement);
        this.session.execute(createStatement);
        if (this.indexAnnotation != null) {
            this.initIndexQuery();
        }

        this.findPersistedKeys();
    }

    private void findPersistedKeys() {
        String checkStatement = this.cassandraStoreConfig.getTableValidityQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName);
        ResultSet result = this.session.execute(checkStatement);
        List<TableMeta> tableColumns = new ArrayList();
        Iterator var4 = result.iterator();

        while (var4.hasNext()) {
            Row row = (Row) var4.next();
            TableMeta tableMeta = new TableMeta(row.getString("column_name"), row.getString("validator"), row.getString("type"));
            tableColumns.add(tableMeta);
        }

        this.persistedKeyColumns = new HashMap();
        tableColumns.stream().filter((column) -> column.getKeyType().equals("partition_key") || column.getKeyType().equals("clustering")).forEach((column) -> {
            String var10000 = this.persistedKeyColumns.put(column.getColumnName(), column.getDataType());
        });
    }

    private Map<String, String> initPersistedColumns(List<TableMeta> tableColumns) {
        this.persistedKeyColumns = new HashMap();
        Map<String, String> persistedColumns = new HashMap();
        tableColumns.forEach((column) -> {
            if (column.getKeyType().equals("partition_key") || column.getKeyType().equals("clustering")) {
                this.persistedKeyColumns.put(column.getColumnName(), column.getDataType());
            }

            persistedColumns.put(column.getColumnName(), column.getDataType());
        });
        return persistedColumns;
    }

    private boolean checkKeyAndDataTypeValidity(Map<String, String> tableDet, Map<String, String> persistedColumns) {
        Iterator var5 = this.schema.iterator();

        boolean validColumns;
        do {
            Attribute attribute;
            if (!var5.hasNext()) {
                var5 = this.schema.iterator();

                boolean validDataTypes;
                do {
                    if (!var5.hasNext()) {
                        return this.isValidKeys(tableDet);
                    }

                    attribute = (Attribute) var5.next();
                    String persistedColumnType = (String) persistedColumns.get(attribute.getName().toLowerCase(Locale.ENGLISH));
                    String inComingDataType = CassandraTableUtils.convertToCassandraDataTypes(attribute.getType());
                    validDataTypes = persistedColumnType.equalsIgnoreCase(inComingDataType);
                } while (validDataTypes);

                return false;
            }

            attribute = (Attribute) var5.next();
            validColumns = tableDet.containsKey(attribute.getName().toLowerCase(Locale.ENGLISH));
        } while (validColumns);

        return false;
    }

    private boolean isValidKeys(Map<String, String> tableDet) {
        boolean validKeys = false;
        if (this.noKeys) {
            validKeys = tableDet.containsKey("_id");
            this.noKeyTable = validKeys;
        } else {
            Iterator var3 = this.primaryKeys.iterator();

            while (var3.hasNext()) {
                Attribute attribute = (Attribute) var3.next();
                String pk = attribute.getName().toLowerCase(Locale.ENGLISH);
                validKeys = tableDet.containsKey(pk) && (((String) tableDet.get(pk)).equals("partition_key") || ((String) tableDet.get(pk)).equals("clustering"));
                if (!validKeys) {
                    return false;
                }
            }
        }

        return validKeys;
    }

    private void initIndexQuery() {
        String[] derivedIndexes = ((Element) this.indexAnnotation.getElements().get(0)).getValue().split(",");
        this.indexes = new ArrayList();
        Arrays.stream(derivedIndexes).forEach((index) -> {
            this.schema.stream().filter((attribute) -> {
                return attribute.getName().trim().equals(index);
            }).forEach((attribute) -> {
                String indexQuery = this.cassandraStoreConfig.getIndexQuery().replace("{{KEYSPACE}}", this.keyspace).replace("{{TABLE}}", this.tableName).replace("{{INDEX}}", index);
                this.session.execute(indexQuery);
                this.indexes.add(index);
            });
        });
    }
}
