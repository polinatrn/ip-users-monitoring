# Event Monitoring Topology

This project implements an Apache Storm topology for real-time event monitoring and alerting.

## Overview

The EventMonitoringTopology is designed to process incoming events, persist them to a database, perform user and IP counts, and generate alerts based on specified thresholds.

## Components

1. **EventSpout**: Generates events.
2. **EventPersistenceBolt**: Persists events to a database.
3. **CountUsersAndIpsBolt**: Counts distinct users per IP and distinct IPs per user.
4. **AlertBolt**: Generates alerts based on combined threshold.

## Topology Flow

```mermaid
graph LR
    A[EventSpout] --> B[EventPersistenceBolt]
    B --> C[CountUsersAndIpsBolt]
    C --> D[AlertBolt]
```

## Note
This topology is designed for demonstration purposes using a LocalCluster and H2 in-memory database. For production deployment, configure the topology for a distributed Storm cluster and .
