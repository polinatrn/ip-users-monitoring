package org.example;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For topology-related code reusage.
 */
public abstract class AbstractUserTopology {
    private static final String EVENTS_TABLE = "events";

    private static final List<String> setupSqls = Lists.newArrayList(
"CREATE TABLE IF NOT EXISTS " + EVENTS_TABLE + " (user_id VARCHAR(255), ip VARCHAR(255));",
            "CREATE INDEX ix_" + EVENTS_TABLE + "_user_id_ip ON events (user_id, ip);"
    );
    protected JdbcMapper jdbcMapper;
    protected ConnectionProvider connectionProvider;

    protected static final String JDBC_CONF = "jdbc.conf";
    protected static final String SELECT_IP_PER_USER_QUERY = "SELECT user_id, count(distinct ip) as ip_count FROM " + EVENTS_TABLE + " WHERE user_id=? GROUP BY user_id limit 1";
    protected static final String SELECT_USERS_PER_IP_QUERY = "SELECT ip, count(distinct user_id) as user_count FROM " + EVENTS_TABLE + " WHERE ip=? GROUP BY ip limit 1";


    /**
     * A main method template to extend.
     * @param args main method arguments
     * @throws Exception any expection occuring durch cluster setup or operation
     */
        public void execute(String[] args) throws Exception {
    //        if (args.length != 4 && args.length != 5) {
    //            System.out.println("Usage: " + this.getClass().getSimpleName() + " <dataSourceClassName> <dataSource.url> "
    //                    + "<user> <password> [topology name]");
    //            System.exit(-1);
    //        }
            Map<String, Object> map = Maps.newHashMap();
    //        map.put("dataSourceClassName", args[0]); //com.mysql.jdbc.jdbc2.optional.MysqlDataSource
            map.put("jdbcUrl", "jdbc:h2:mem:testdb"); //jdbc:mysql://localhost/test
            map.put("dataSource.user", "sa"); //root
            map.put("dataSource.password",""); //password

//            Map<String, Object> hikariConfigMap = new HashMap<>();
//            hikariConfigMap.put("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
//            hikariConfigMap.put("dataSource.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
//            hikariConfigMap.put("dataSource.user", "sa");
//            hikariConfigMap.put("dataSource.password", "");
//            connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

            Config config = new Config();
            config.put(JDBC_CONF, map);

            connectionProvider = new HikariCPConnectionProvider(map);
            connectionProvider.prepare();
            int queryTimeoutSecs = 60;
            JdbcClient jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
            for (String sql : setupSqls) {
                System.out.println("Executing SQL: " + sql);
                jdbcClient.executeSql(sql);
            }

        this.jdbcMapper = new SimpleJdbcMapper(EVENTS_TABLE, connectionProvider);
//        connectionProvider.cleanup();
//        this.connectionProvider = new HikariCPConnectionProvider(map);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("event-monitoring-topology", config, getTopology());
    }

    public abstract StormTopology getTopology();

}
