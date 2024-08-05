package org.example;


import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

import static org.example.SqlQueries.*;

public class AlertBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AlertBolt.class);
    private OutputCollector collector;
    private final int threshold;
    private final JedisPoolConfig poolConfig;
    private Jedis jedis;

    public AlertBolt(int threshold, JedisPoolConfig poolConfig) {
        this.threshold = threshold;
        this.poolConfig = poolConfig;

    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.jedis = new Jedis(poolConfig.getHost(), poolConfig.getPort());

    }

    @Override
    public void execute(Tuple tuple) {
        String ip = tuple.getStringByField(IP);
        long userCount = tuple.getLongByField(USER_COUNT);
        String user = tuple.getStringByField(USER_ID);
        long ipCount = tuple.getLongByField(IP_COUNT);

        String key = "alert:" + user + ":" + ip + ":IP count: " + ipCount + ":User count: " + userCount + ":threshold: " + threshold;

        if (ipCount + userCount > threshold && !jedis.exists(key)) {
            LOG.warn("Dangerous event: {\"{}\":\"{}\"}! IP count = {} User Count = {}. Crossed the threshold of {}", user, ip, ipCount, userCount, threshold);
            jedis.set(key, "");

            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
    @Override
    public void cleanup() {
        jedis.close();
    }
}

