# Event Monitoring Topology

This project implements an Apache Storm topology for real-time event monitoring and alerting.

## Overview

The EventMonitoringTopology is designed to process incoming events, persist them to a database, perform user and IP counts, and generate alerts based on specified thresholds.

## Components

1. **event_spout**: Generates events from local file.
2. **ip_to_user_persistence_bolt**: Adds events to a redis set of ip as a key.
3. **user_to_ip_persistence_bolt**: Adds events to a redis set of user as a key.
4. **count_users_and_ips_bolt**: Fetches set sizes of ip and user sets and emits them.
5**AlertBolt**: Generates alerts based on combined threshold. Deduplicates identical alerts.

## Setup
1. Download Redis if not exists: `brew install redis`
2. Start Redis server `redis-server`
3. Before each run for testing purposes, clean the DB `redis-cli FLUSHDB`

## Topology Flow

```
                   +-------------+
                   |  EventSpout |
                   +-------------+
                          |
                          | (shuffle grouping)
                          |
            +-------------+-------------+
            |                           |
 +---------------------+   +---------------------+
 | IpToUserPersistence |   | UserToIpPersistence |
 |        Bolt         |   |        Bolt         |
 +---------------------+   +---------------------+
            |                           |
            |                           |
            | (shuffle grouping)        | (shuffle grouping)
            |                           |
            +-------------+-------------+
                          |
                          v
            +-----------------------------+
            |     CountUsersAndIpsBolt    |
            +-----------------------------+
                          |
                          | (shuffle grouping)
                          v
                   +-------------+
                   |  AlertBolt  |
                   +-------------+

```

## Note
This topology is designed for demonstration purposes using a LocalCluster and a local Redis database. For production deployment, configure the topology for a distributed Storm cluster and Redis server.
