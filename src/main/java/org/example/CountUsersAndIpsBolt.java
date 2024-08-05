package org.example;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

import static org.example.FieldNames.*;

public class CountUsersAndIpsBolt extends BaseRichBolt {
    private Jedis jedis;
    private final JedisPoolConfig poolConfig;
    private OutputCollector collector;

    public CountUsersAndIpsBolt(JedisPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.jedis = new Jedis(poolConfig.getHost(), poolConfig.getPort());
    }

    @Override
    public void execute(Tuple input) {
        String ip = input.getStringByField(IP);
        String userId = input.getStringByField(USER_ID);

        jedis.sadd("IP:" + ip, userId);
        jedis.sadd("User:" + userId, ip);

        Long userCount = jedis.scard("IP:" + ip);
        Long ipCount = jedis.scard("User:" + userId);

        this.collector.emit(new Values(userId, ip, userCount, ipCount));
    }

    @Override
    public void cleanup() {
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(USER_ID, IP, USER_COUNT, IP_COUNT));
    }
}