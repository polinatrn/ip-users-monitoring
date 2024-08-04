package org.example;

import com.google.common.collect.Lists;

import java.util.List;

public class SqlQueries {
    static final String EVENTS_TABLE = "events";
    static final String USER_ID = "user_id";
    static final String IP = "ip";
    static final String USER_COUNT = "user_count";
    static final String IP_COUNT = "ip_count";
    static final List<String> setupSqls = Lists.newArrayList("CREATE TABLE IF NOT EXISTS " + EVENTS_TABLE + " (" + USER_ID + " VARCHAR(255), " + IP + " VARCHAR(255));",
            "CREATE INDEX ix_" + EVENTS_TABLE + "_" + USER_ID + "_" + IP + " ON events (user_id, ip);");
    static final String SELECT_IP_PER_USER_QUERY = "SELECT count(distinct " + IP + ") as "+ IP_COUNT +" FROM " + EVENTS_TABLE +
            " WHERE " + USER_ID +"=? GROUP BY user_id limit 1";
    static final String SELECT_USERS_PER_IP_QUERY = "SELECT count(distinct "+ USER_ID + ") as "+ USER_COUNT +" FROM " + EVENTS_TABLE +
            " WHERE " + IP + "=? GROUP BY ip limit 1";
    static final String COMBINED_SELECT = "SELECT (" + SELECT_IP_PER_USER_QUERY + ") as " + IP_COUNT + ", (" + SELECT_USERS_PER_IP_QUERY + ") as " + USER_COUNT;
    static final String INSERT_INTO_EVENTS = "insert into events (" + USER_ID + ", " + IP + ") values (?,?)";
}
