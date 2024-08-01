/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.google.common.collect.Lists;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;


public class EventPersistenceTopology extends AbstractUserTopology {
    private static final String EVENT_PERSISTENCE_BOLT = "event_persistence_bolt";
    private static final String EVENT_SPOUT = "event_spout";
    private static final String COUNT_USERS_PER_IP_BOLT = "count-users-per-ip-bolt";
    private static final String COUNT_IPS_PER_USER_BOLT = "count-ips-per-user-bolt";
    private static final String ALERT_BOLT = "alert-bolt";
    private final static int COMBINED_THRESHOLD = 2;


    public static void main(String[] args) throws Exception {
        new EventPersistenceTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
        //must specify column schema when providing custom query.
        List<Column> schemaColumns = Lists.newArrayList(
                new Column("user_id", Types.VARCHAR),
                new Column("ip", Types.VARCHAR));
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);

        JdbcInsertBolt eventPersistenceBolt = new EventPersistenceBolt(connectionProvider, mapper)
                .withInsertQuery("insert into events (user_id, ip) values (?,?)").withQueryTimeoutSecs(30);


        Fields outputFields = new Fields("ip", "user_count");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("ip", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupUsersPerIpMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt countUsersPerIpBolt = new JdbcLookupBolt(connectionProvider, SELECT_USERS_PER_IP_QUERY, jdbcLookupUsersPerIpMapper).withQueryTimeoutSecs(30);

        outputFields = new Fields("user_id", "ip_count");
        queryParamColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupIpPerUserMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt countIpsPerUserBolt = new JdbcLookupBolt(connectionProvider, SELECT_IP_PER_USER_QUERY, jdbcLookupIpPerUserMapper).withQueryTimeoutSecs(30);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT_SPOUT, new EventSpout());
        builder.setBolt(EVENT_PERSISTENCE_BOLT, eventPersistenceBolt, 1).shuffleGrouping(EVENT_SPOUT);
        builder.setBolt(COUNT_USERS_PER_IP_BOLT, countUsersPerIpBolt,1).shuffleGrouping(EVENT_PERSISTENCE_BOLT);
        builder.setBolt(COUNT_IPS_PER_USER_BOLT, countIpsPerUserBolt,1).shuffleGrouping(EVENT_PERSISTENCE_BOLT);
        builder.setBolt(ALERT_BOLT, new AlertBolt(COMBINED_THRESHOLD), 1)
                .shuffleGrouping(COUNT_USERS_PER_IP_BOLT)
                .shuffleGrouping(COUNT_IPS_PER_USER_BOLT);
        return builder.createTopology();
    }
}
