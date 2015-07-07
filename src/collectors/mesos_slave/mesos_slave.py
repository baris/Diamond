#! /usr/bin/python
#
# Copyright 2015 Ray Rodriguez
# Copyright 2015 Baris Metin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Diamond mesos slave collector based on the collectd plugin at
https://raw.githubusercontent.com/rayrod2030/collectd-mesos/master/mesos-slave.py
"""

import collections
import diamond.collector
import json
import socket
import urllib2

Stat = collections.namedtuple('Stat', ('type', 'path'))


# DICT: Common Metrics in 0.19.0, 0.20.0 and 0.21.0
STATS_MESOS = {
    # Slave
    'slave/frameworks_active': Stat("gauge", "slave/frameworks_active"),
    'slave/invalid_framework_messages': Stat("counter", "slave/invalid_framework_messages"),
    'slave/invalid_status_updates': Stat("counter", "slave/invalid_status_updates"),
    'slave/recovery_errors': Stat("counter", "slave/recovery_errors"),
    'slave/registered': Stat("gauge", "slave/registered"),
    'slave/tasks_failed': Stat("counter", "slave/tasks_failed"),
    'slave/tasks_finished': Stat("counter", "slave/tasks_finished"),
    'slave/tasks_killed': Stat("counter", "slave/tasks_killed"),
    'slave/tasks_lost': Stat("counter", "slave/tasks_lost"),
    'slave/tasks_running': Stat("gauge", "slave/tasks_running"),
    'slave/tasks_starting': Stat("gauge", "slave/tasks_starting"),
    'slave/tasks_staging': Stat("gauge", "slave/tasks_staging"),
    'slave/uptime_secs': Stat("gauge", "slave/uptime_secs"),
    'slave/valid_framework_messages': Stat("counter", "slave/valid_framework_messages"),
    'slave/valid_status_updates': Stat("counter", "slave/valid_status_updates"),

    # System
    'system/cpus_total': Stat("gauge", "system/cpus_total"),
    'system/load_15min': Stat("gauge", "system/load_15min"),
    'system/load_1min': Stat("gauge", "system/load_1min"),
    'system/load_5min': Stat("gauge", "system/load_5min"),
    'system/mem_free_bytes': Stat("bytes", "system/mem_free_bytes"),
    'system/mem_total_bytes': Stat("bytes", "system/mem_total_bytes")
}

# DICT: Mesos 0.19.0, 0.19.1
STATS_MESOS_019 = {
}

# DICT: Mesos 0.20.0, 0.20.1
STATS_MESOS_020 = {
    'slave/executors_registering': Stat("gauge", "slave/executors_registering"),
    'slave/executors_running': Stat("gauge", "slave/executors_running"),
    'slave/executors_terminating': Stat("gauge", "slave/executors_terminating"),
    'slave/executors_terminated': Stat("counter", "slave/executors_terminated")
}

# DICT: Mesos 0.21.0, 0.21.1
STATS_MESOS_021 = {
    'slave/cpus_percent': Stat("percent", "slave/cpus_percent"),
    'slave/cpus_total': Stat("gauge", "slave/cpus_total"),
    'slave/cpus_used': Stat("gauge", "slave/cpus_used"),
    'slave/disk_percent': Stat("percent", "slave/disk_percent"),
    'slave/disk_total': Stat("gauge", "slave/disk_total"),
    'slave/disk_used': Stat("gauge", "slave/disk_used"),
    'slave/mem_percent': Stat("percent", "slave/mem_percent"),
    'slave/mem_total': Stat("gauge", "slave/mem_total"),
    'slave/mem_used': Stat("gauge", "slave/mem_used"),
    'slave/executors_registering': Stat("gauge", "slave/executors_registering"),
    'slave/executors_running': Stat("gauge", "slave/executors_running"),
    'slave/executors_terminating': Stat("gauge", "slave/executors_terminating"),
    'slave/executors_terminated': Stat("counter", "slave/executors_terminated")
}


class MesosSlaveCollector(diamond.collector.Collector):

    def get_default_config(self):
        """Return the default collector settings"""
        config = super(MesosSlaveCollector, self).get_default_config()
        config.update({
            'host': socket.gethostbyname(socket.gethostname()),
            'port': "5051",
            'mesos_version': '0.20.1',
            'url': "",
        })
        config['url'] = "http://" + config['host'] + ":" + str(config['port']) + "/metrics/snapshot"
        return config

    def set_mesos_stats(self):
        version = self.config['mesos_version']
        if version == "0.19.0" or version == "0.19.1":
            STATS_CUR = dict(STATS_MESOS.items() + STATS_MESOS_019.items())
        elif version == "0.20.0" or version == "0.20.1":
            STATS_CUR = dict(STATS_MESOS.items() + STATS_MESOS_020.items())
        elif version == "0.21.0" or version == "0.21.1":
            STATS_CUR = dict(STATS_MESOS.items() + STATS_MESOS_021.items())
        else:
            STATS_CUR = dict(STATS_MESOS.items() + STATS_MESOS_021.items())
        self.STATS_CUR = STATS_CUR

    def collect(self):
        self.set_mesos_stats()
        self.fetch_stats()

    def fetch_stats(self):
        url = self.config['url']
        try:
            result = json.load(urllib2.urlopen(url, timeout=10))
        except urllib2.URLError, e:
            self.log.error('mesos-slave plugin: Error connecting to %s - %r' % (url, e))
            return None
        return self.parse_stats(result)

    def parse_stats(self, data):
        """Parse stats response from Mesos slave"""
        for name, key in self.STATS_CUR.iteritems():
            result = self.__lookup_stat(name, data)
            self.dispatch_stat(result, name, key)

    def dispatch_stat(self, result, name, key):
        """Read a key from info response data and dispatch a value"""
        if result is None:
            self.log.warn('mesos-slave plugin: Value not found for %s' % name)
            return
        estype = key.type
        value = result
        self.log.info('Sending value[%s]: %s=%s' % (estype, name, value))

        if estype == "counter":
            value = self.derivative(name, value)
        if value < 0:
            value = 0
        self.publish(name, value)
        # val = collectd.Values(plugin='mesos-slave')
        # val.type = estype
        # val.type_instance = name
        # val.values = [value]
        # val.dispatch()

    def __lookup_stat(self, stat, json):
        val = self.__dig_it_up(json, self.STATS_CUR[stat].path)

        # Check to make sure we have a valid result
        # dig_it_up returns False if no match found
        if not isinstance(val, bool):
            return val
        else:
            return None

    def __dig_it_up(self, obj, path):
        try:
            if type(path) in (str, unicode):
                path = path.split('.')
            return reduce(lambda x, y: x[y], path, obj)
        except:
            return False
