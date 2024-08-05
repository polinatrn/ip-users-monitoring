package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;

import static org.example.FieldNames.IP;
import static org.example.FieldNames.USER_ID;


public class EventMonitoringTopology {

    static final String USER_TO_IP_PERSISTENCE_BOLT = "user_to_ip_persistence_bolt";
    static final String IP_TO_USER_PERSISTENCE_BOLT = "ip_to_user_persistence_bolt";
    private static final String EVENT_SPOUT = "event_spout";
    private static final String COUNT_USERS_AND_IPS_BOLT = "count-users-and-ips-bolt";
    private static final String ALERT_BOLT = "alert-bolt";
    private static final int COMBINED_THRESHOLD = 5;


    public static void main(String[] args) throws Exception {
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379).build();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("event-monitoring-topology", new Config(), getTopology(poolConfig));
    }


    public static StormTopology getTopology(JedisPoolConfig poolConfig) {

        RedisStoreBolt ipToUserPersistenceBolt = new RedisEventStoreBolt(poolConfig, new RedisEventStoreMapper(IP, USER_ID));
        RedisStoreBolt userToIpPersistenceBolt = new RedisEventStoreBolt(poolConfig, new RedisEventStoreMapper(USER_ID, IP));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT_SPOUT, new EventSpout(), 1);
        builder.setBolt(IP_TO_USER_PERSISTENCE_BOLT, ipToUserPersistenceBolt, 2).setNumTasks(4)
                .shuffleGrouping(EVENT_SPOUT);
        builder.setBolt(USER_TO_IP_PERSISTENCE_BOLT, userToIpPersistenceBolt, 2).setNumTasks(4)
                .shuffleGrouping(EVENT_SPOUT);
        builder.setBolt(COUNT_USERS_AND_IPS_BOLT, new CountUsersAndIpsBolt(poolConfig), 2).setNumTasks(4)
                .shuffleGrouping(USER_TO_IP_PERSISTENCE_BOLT)
                .shuffleGrouping(IP_TO_USER_PERSISTENCE_BOLT);
        builder.setBolt(ALERT_BOLT, new AlertBolt(COMBINED_THRESHOLD, poolConfig), 2).setNumTasks(4)
                .shuffleGrouping(COUNT_USERS_AND_IPS_BOLT);
        return builder.createTopology();
    }
}
