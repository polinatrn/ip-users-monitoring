# Event Monitoring Topology

This project implements an Apache Storm topology for real-time event monitoring and alerting.

## Overview

The EventMonitoringTopology is designed to process incoming events, persist them to a database, perform user and IP counts, and generate alerts based on specified thresholds.

## Components

1. **EventSpout**: Generates events.
2. **EventPersistenceBolt**: Persists events to a database.
3. **CountUsersAndIpsBolt**: Counts distinct users per IP and distinct IPs per user.
4. **AlertBolt**: Generates alerts based on combined threshold.

## Setup
1. Download Redis if not exists: `brew install redis`
2. Start Redis server `redis-server`
3. Before each run for testing purposes, clean the DB `redis-cli FLUSHDB`

## Topology Flow

```
[ event_spout ]
       |
       v
[ ip_to_user_persistence_bolt ]
       |
       v
[ user_to_ip_persistence_bolt ]
       |
       v
[ count_users_and_ips_bolt ]
       |
       v
[ alert_bolt ]

```

## Note
This topology is designed for demonstration purposes using a LocalCluster and local Redis database. For production deployment, configure the topology for a distributed Storm cluster and Redis server.
