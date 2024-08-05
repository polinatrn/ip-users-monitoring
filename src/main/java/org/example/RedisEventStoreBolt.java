package org.example;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import redis.clients.jedis.Jedis;

import java.util.Map;

import static org.example.FieldNames.*;


public class RedisEventStoreBolt extends RedisStoreBolt {
    JedisPoolConfig poolConfig;
    TopologyContext topologyContext;
    public RedisEventStoreBolt(JedisPoolConfig poolConfig, RedisStoreMapper storeMapper) {
        super(poolConfig, storeMapper);
        this.poolConfig = poolConfig;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.topologyContext = context;

    }
    @Override
    public void execute(Tuple input) {
        super.execute(input);
        String eventId = input.getStringByField(EVENT_ID);
        setFinishedEventId(eventId);
        collector.emit(input, new Values(
                eventId,
                input.getStringByField(USER_ID),
                input.getStringByField(IP)));
        collector.ack(input);
    }

    private void setFinishedEventId(String eventId) {
        try (Jedis jedis = new Jedis(poolConfig.getHost(), poolConfig.getPort())){
            jedis.sadd(eventId, topologyContext.getThisComponentId());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_ID,USER_ID, IP));
    }

}
