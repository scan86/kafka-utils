__author__ = 'mkiselev'

import sys
import yaml
import time
import paramiko
import argparse
from kazoo.client import KazooClient

config = {}


def wait_until_status(ssh, expected_status):
    tries = 0
    while True:
        print "check kafka status: attempt %s" % tries

        stdin, stdout, stderr = ssh.exec_command("sudo /etc/init.d/kafka status")
        status = stdout.read().strip()
        print "Kafka status: %s" % status

        if status == expected_status:
            break
        else:
            if tries >= 5:
                print "'%s' after 5 tries... Exiting..." % status
                sys.exit()

            time.sleep(15)
            tries += 1


def stop_kafka(ssh, host):
    print "Stoping kafka on %s" % host

    if config['monit']:
        print "use monit command"
        ssh.exec_command("sudo monit stop kafka")
    else:
        ssh.exec_command("sudo /etc/init.d/kafka stop")

    wait_until_status(ssh, 'kafka is NOT running.')


def start_kafka(ssh, host):
    print "Staring kafka on %s" % host

    if config['monit']:
        print "use monit command"
        ssh.exec_command("sudo monit start kafka")
    else:
        ssh.exec_command("sudo /etc/init.d/kafka start")

    wait_until_status(ssh, 'kafka is running.')


def delete_topics_data(ssh, host):
    print "Deleting topics data on '%s'..." % host
    ssh.exec_command("sudo rm -r /var/spool/kafka*/*")
    print "Deleted"


def delete_zk_data():
    print "Deleting zk data..."

    zk = KazooClient(hosts=config['zk'])
    zk.start()

    zk.delete("/kafka/brokers/topics", recursive=True)
    zk.delete("/kafka/consumers", recursive=True)

    zk.stop()

    print "Deleted zk data."


def main():
    parser = argparse.ArgumentParser(description='Remove kafka topics')
    parser.add_argument('config', help="Path to configuration yaml file.")
    args = parser.parse_args()

    print "Loading %s..." % args.config

    global config
    config = yaml.safe_load(open(args.config).read())

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )

    # TODO: Run in parallel
    for host in config['brokers']:
        ssh.connect(host, username=config['user'], password=config['password'])
        stop_kafka(ssh, host)
        delete_topics_data(ssh, host)
        ssh.close()

    delete_zk_data()

    # # TODO: Run in parallel
    for host in config['brokers']:
        ssh.connect(host, username=config['user'], password=config['password'])
        start_kafka(ssh, host)
        ssh.close()

#
# The "main" entry
#
if __name__ == '__main__':
    sys.exit(main())