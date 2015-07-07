#!/usr/bin/env python

"""
Diamond marathon collector fetches metrics from marathon's HTTP
metrics endpoint an publishes them.

#### Example Configuration
```
    host = localhost
    port = 5052
    username = 'admin'
    password = 'password'
```
"""

import diamond.collector
import json
import urllib2


class MarathonCollector(diamond.collector.Collector):

    def get_default_config(self):
        config = super(MarathonCollector, self).get_default_config()
        config.update({
            'host': 'localhost',
            'port': 5052,
            'username': '',
            'password': '',
        })
        return config

    def get_metrics(self):
        url = "http://{0}:{1}/metrics".format(self.config['host'], self.config['port'])
        if self.config['username'] and self.config['password']:
            url = "http://{0}:{1}@{2}:{3}/metrics".format(
                self.config['username'],
                self.config['password'],
                self.config['host'],
                self.config['port']
            )
        try:
            return json.load(urllib2.urlopen(url))
        except (urllib2.HTTPError, ValueError), err:
            self.log.error("Unable to read JSON response: {0}".format(err))
            return {}

    def collect(self):
        metrics = self.get_metrics()
        for group in ['gauges', 'histograms', 'meters', 'timers', 'counters']:
            for name,values in metrics.get(group, {}).items():
                for metric, value in values.items():
                    if not isinstance(value, basestring):
                        self.publish('.'.join((name, metric)), value)
