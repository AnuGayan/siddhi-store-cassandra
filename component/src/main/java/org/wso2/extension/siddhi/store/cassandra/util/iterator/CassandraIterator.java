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
package org.wso2.extension.siddhi.store.cassandra.util.iterator;

import com.datastax.driver.core.Row;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A class representing a RecordIterator which is responsible for processing Cassandra Event Table find() operations in
 * a streaming fashion.
 */
public class CassandraIterator implements RecordIterator<Object[]> {

    private Iterator<Row> iterator;
    private List<Attribute> attributes;

    public CassandraIterator(Iterator<Row> iterator, List<Attribute> attributes) {

        this.iterator = iterator;
        this.attributes = attributes;
    }

    @Override
    public void close() throws IOException {
        // Not supported in cassandra
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Object[] next() {
        Row row = iterator.next();
        return extractRecord(row);
    }

    /**
     * Method which is used for extracting record values (in the form of an Object array) from an CQL {@link Row},
     * according to the table's field type order.
     *
     * @param row the {@link Row} from which the values should be retrieved.
     * @return an array of extracted values, all cast to {@link Object} type for portability.
     *             to the table definition
     */
    private Object[] extractRecord(Row row) {
        List<Object> result = new ArrayList<>();
        this.attributes.forEach(attribute -> {
            switch (attribute.getType()) {
                case BOOL:
                    result.add(row.getBool(attribute.getName()));
                    break;
                case DOUBLE:
                    result.add(row.getDouble(attribute.getName()));
                    break;
                case FLOAT:
                    result.add(row.getFloat(attribute.getName()));
                    break;
                case INT:
                    result.add(row.getInt(attribute.getName()));
                    break;
                case LONG:
                    result.add(row.getLong(attribute.getName()));
                    break;
                case STRING:
                    result.add(row.getString(attribute.getName()));
                    break;
                default:
                    //Operation not supported
            }
        });
        return result.toArray();
    }
}
