# kafka-utils

# remove-kafka-topics

### Description

Only for test environments.
Pain related to https://issues.apache.org/jira/browse/KAFKA-1397

requires: pyyaml, paramiko, argparse, kazoo

### Usage

```
usage: remove-kafka-topics.py [-h] config

Remove kafka topics

positional arguments:
  config      Path to configuration yaml file.

optional arguments:
  -h, --help  show this help message and exit
```

### Configuration file example

```
---
brokers:
  - broker1
  - broker2
  - broker3

zk: 'zk-node1:2181,zk-node2:2181,zk-node3:2181'
user: clusterLogin
password: clusterPassword

monit: True
```