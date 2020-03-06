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
package org.wso2.extension.siddhi.store.cassandra.util;

/**
 * Class for maintaining constants used by the Cassandra table implementation.
 */
public class CassandraEventTableConstants {

    private CassandraEventTableConstants(){
        //prevents the object initialization
    }

    //Constants that are needed to initiate cassandra connection
    public static final String ANNOTATION_ELEMENT_TABLE_NAME = "column.family";
    public static final String ANNOTATION_CLIENT_PORT = "client.port";
    public static final String ANNOTATION_ELEMENT_KEY_SPACE = "keyspace";
    public static final String ANNOTATION_HOST = "cassandra.host";
    public static final String DEFAULT_KEY_SPACE = "wso2sp";
    public static final int DEFAULT_PORT = 9042;
    public static final String ANNOTATION_USER_NAME = "username";
    public static final String ANNOTATION_PASSWORD = "password";

    //Miscellaneous CQL constants
    public static final String CQL_EQUALS = "=";
    public static final String CQL_FILTERING = "ALLOW FILTERING";
    public static final String CQL_LESS_THAN = "<";
    public static final String CQL_GREATER_THAN = ">";
    public static final String CQL_LESS_THAN_EQUAL = "<=";
    public static final String CQL_GREATER_THAN_EQUAL = ">=";
    public static final String CQL_COMPARE_EQUAL = "=";
    public static final String CQL_AND = "AND";
    public static final String CQL_ID = "\"_id\"";
    public static final String CQL_IN = "IN";
    public static final String CQL_PRIMARY_KEY_DEF = "PRIMARY KEY";
    public static final String CQL_TEXT = "text";
    public static final String CQL_WHERE = "WHERE";
    public static final String WHITESPACE = " ";
    public static final String SEPARATOR = ",";
    public static final String QUESTION_MARK = "?";
    public static final String OPEN_PARENTHESIS = "(";
    public static final String OPEN_SQUARE_BRACKET = "[";
    public static final String CLOSE_PARENTHESIS = ")";
    public static final String CLOSE_SQUARE_BRACKET = "]";
    public static final String CONDITION_CQL_ID = "\"_id\"=?";
    public static final String CONFIG_FILE = "cassandra-table-config.xml";

    public static final String EXCEPTION_MATH_OPERATOR = "Math operators at the beginning of an operation is " +
            "not supported in Cassandra ";
    public static final String EXCEPTION_OR_OPERATOR = "supported in cassandra OR operator is not " +
            "supported in cassandra";
    public static final String EXCEPTION_NOT_OPERATOR = "NOT_EQUAL operator is not supported in cassandra";

    public static final String PLACEHOLDER_COLUMNS = "{{COLUMNS}}";
    public static final String PLACEHOLDER_PRIMARY_KEYS = "{{PRIMARY_KEYS}}";
    public static final String PLACEHOLDER_COLUMNS_AND_VALUES = "{{COLUMNS_AND_VALUES}}";
    public static final String PLACEHOLDER_CONDITION = "{{CONDITION}}";
    public static final String PLACEHOLDER_INDEX = "{{INDEX}}";
    public static final String PLACEHOLDER_INSERT_VALUES = "{{INSERT_VALUES}}";
    public static final String PLACEHOLDER_KEYSPACE = "{{KEYSPACE}}";
    public static final String PLACEHOLDER_QUESTION_MARKS = "{{QUESTION_MARKS}}";
    public static final String PLACEHOLDER_TABLE = "{{TABLE}}";
    public static final String PLACEHOLDER_SELECT_VALUES = "{{SELECT_VALUES}}";

    public static final String TABLE_PROPERTY_CLUSTERING_KEY = "clustering";
    public static final String TABLE_PROPERTY_COLUMN_NAME = "column_name";
    public static final String TABLE_PROPERTY_KIND = "validator";
    public static final String TABLE_PROPERTY_PARTITION_KEY = "partition_key";
    public static final String TABLE_PROPERTY_TYPE = "type";
}
