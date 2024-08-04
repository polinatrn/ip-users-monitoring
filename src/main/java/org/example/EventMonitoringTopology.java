package org.example;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.example.SqlQueries.*;


public class EventMonitoringTopology {

    private static final String EVENT_PERSISTENCE_BOLT = "event_persistence_bolt";
    private static final String EVENT_SPOUT = "event_spout";
    private static final String COUNT_USERS_AND_IPS_BOLT = "count-users-and-ips-bolt";
    private static final String ALERT_BOLT = "alert-bolt";
    private static final int COMBINED_THRESHOLD = 5;
    private static ConnectionProvider connectionProvider;


    public static void main(String[] args) throws Exception {
        Config config = setupDb();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("event-monitoring-topology", config, getTopology());
    }

    private static Config setupDb() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("jdbcUrl", "jdbc:h2:mem:testdb");
        map.put("dataSource.user", "sa");
        map.put("dataSource.password", "");

        Config config = new Config();
        config.put("jdbc.conf", map);

        connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();
        int queryTimeoutSecs = 60;
        JdbcClient jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
        for (String sql : setupSqls) {
            System.out.println("Executing SQL: " + sql);
            jdbcClient.executeSql(sql);
        }
        return config;
    }


    public static StormTopology getTopology() {
        //must specify column schema when providing custom query.
        List<Column> schemaColumns = Lists.newArrayList(new Column(USER_ID, Types.VARCHAR), new Column(IP, Types.VARCHAR));
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);

        JdbcInsertBolt eventPersistenceBolt = new EventPersistenceBolt(connectionProvider, mapper)
                .withInsertQuery(INSERT_INTO_EVENTS).withQueryTimeoutSecs(30);


        Fields outputFields = new Fields(USER_ID, IP, USER_COUNT, IP_COUNT);
        List<Column> queryParamColumns = Lists.newArrayList(new Column(USER_ID, Types.VARCHAR),new Column(IP, Types.VARCHAR));
        JdbcLookupMapper jdbcLookupUsersPerIpMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt countUsersAndIpsBolt = new JdbcLookupBolt(connectionProvider, COMBINED_SELECT, jdbcLookupUsersPerIpMapper).withQueryTimeoutSecs(30);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT_SPOUT, new EventSpout(), 1);
        builder.setBolt(EVENT_PERSISTENCE_BOLT, eventPersistenceBolt, 2).shuffleGrouping(EVENT_SPOUT);
        builder.setBolt(COUNT_USERS_AND_IPS_BOLT, countUsersAndIpsBolt, 2).shuffleGrouping(EVENT_PERSISTENCE_BOLT);
        builder.setBolt(ALERT_BOLT, new AlertBolt(COMBINED_THRESHOLD), 2).shuffleGrouping(COUNT_USERS_AND_IPS_BOLT);
        return builder.createTopology();
    }
}
