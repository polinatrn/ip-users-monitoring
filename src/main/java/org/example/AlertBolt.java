package org.example;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.example.SqlQueries.*;

public class AlertBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AlertBolt.class);
    private OutputCollector collector;
    private final int threshold;

    public AlertBolt(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
            String ip = tuple.getStringByField(IP);
            long userCount = tuple.getLongByField(USER_COUNT);
            String user = tuple.getStringByField(USER_ID);
            long ipCount = tuple.getLongByField(IP_COUNT);
            if (ipCount + userCount > threshold) {
                LOG.warn("Dangerous event: {\"{}\":\"{}\"}! IP count = {} User Count = {}. Crossed the threshold of {}",
                        user, ip, ipCount, userCount, threshold);
            }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}

