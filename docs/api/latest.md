# API Docs - v2.0.1-SNAPSHOT

## Store

### cassandra *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected datasource.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="cassandra", cassandra.host="<STRING>", column.family="<STRING>", client.port="<STRING>", keyspace="<STRING>", username="<STRING>", password="<STRING>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">cassandra.host</td>
        <td style="vertical-align: top; word-wrap: break-word">Host that is used to get connected in to the cassandra keyspace.</td>
        <td style="vertical-align: top">localhost</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">column.family</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the event table should be persisted in the store. If no name is specified via this parameter, the event table is persisted with the same name as the Siddhi table.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi Application query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">client.port</td>
        <td style="vertical-align: top; word-wrap: break-word">Client port that is used to get connected with the client store. If no name is specified via this parameter, the default port is taken.</td>
        <td style="vertical-align: top">9042</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">keyspace</td>
        <td style="vertical-align: top; word-wrap: break-word">User need to give the keyspace that the data is persisted. Keyspace name is specified via this parameter.</td>
        <td style="vertical-align: top">cassandraTestTable</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">Through user name user can specify the relevent username that is used to log in to the cassandra keyspace .</td>
        <td style="vertical-align: top">The username of the keyspace</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">Through password user can specify the relevent password that is used to log in to the cassandra keyspace .</td>
        <td style="vertical-align: top">The password of the keyspace</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StockStream (symbol string, price float, volume long); 
@Store(type='cassandra', column.family='StockTable',keyspace='AnalyticsFamily',username='cassandra',password='cassandra',cassandra.host='localhost')@IndexBy('volume')@PrimaryKey('symbol')define table StockTable (symbol string, price float, volume long); 
```
<p style="word-wrap: break-word">This definition creates an event table named <code>StockTable</code> in the AnalyticsFamily keyspace on the Cassandra instance if it does not already exist (with 3 attributes named <code>symbol</code>, <code>price</code>, and <code>volume</code> of the <code>string</code>, <code>float</code> and <code>long</code> types respectively). The connection is made as specified by the parameters configured for the '@Store' annotation. The <code>symbol</code> attribute is considered a unique field, and the values for this attribute are the Cassandra row IDs.</p>

