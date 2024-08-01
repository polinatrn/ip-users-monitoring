package org.example;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private final int threshold;

    public AlertBolt(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.contains("user_count")) {
            String ip = tuple.getStringByField("ip");
            long userCount = tuple.getLongByField("user_count");//todo change to fieldName enum
            if (userCount > threshold) {
                System.out.println("Dangerous event for IP: " + ip + "! User count: " + userCount);
            }
        }
        if (tuple.contains("ip_count")) {
            String user = tuple.getStringByField("user_id");
            long ipCount = tuple.getLongByField("ip_count");
            if (ipCount > threshold) {
                System.out.println("Dangerous event for user: " + user + "! IP count: " + ipCount);
            }
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}

