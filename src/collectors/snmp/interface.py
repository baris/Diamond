# Copyright (C) 2009 Brightcove, Inc. All Rights Reserved. No use,
# copying or distribution of this work may be made except in
# accordance with a valid license agreement from Brightcove, Inc. This
# notice must be included on all copies, modifications and derivatives
# of this work.
#
# Brightcove, Inc MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
# SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. Brightcove
# SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT
# OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS
# DERIVATIVES.
#
# "Brightcove" is a trademark of Brightcove, Inc.

import os
import sys
import string
import logging
import time
import traceback
import configobj
import socket
import re 

# Fix Path    
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"../../")))

from diamond.collector import SNMPCollector
from diamond.metric import Metric

class SNMPInterfaceCollector(SNMPCollector):
    """
    SNMPInterfaceCollector is a SNMP collector for collecting data using SNMP IF-MIB
    """

    # IF-MIB OID
    IF_MIB_INDEX_OID = "1.3.6.1.2.1.2.2.1.1" 
    IF_MIB_NAME_OID = "1.3.6.1.2.1.31.1.1.1.1"
    IF_MIB_TYPE_OID = "1.3.6.1.2.1.2.2.1.3"

    # A list of IF-MIB the 32bit counters to walk
    IF_MIB_GAUGE_OID_TABLE = {'ifInDiscards': "1.3.6.1.2.1.2.2.1.13", 
                                'ifInErrors': "1.3.6.1.2.1.2.2.1.14",
                                'ifOutDiscards': "1.3.6.1.2.1.2.2.1.19", 
                                'ifOutErrors': "1.3.6.1.2.1.2.2.1.20"}

    # A list of IF-MIB 64bit counters to talk 
    IF_MIB_COUNTER_OID_TABLE = {'ifInOctets': "1.3.6.1.2.1.31.1.1.1.6", 
                                'ifInUcastPkts': "1.3.6.1.2.1.31.1.1.1.7", 
                                'ifInMulticastPkts': "1.3.6.1.2.1.31.1.1.1.8", 
                                'ifInBroadcastPkts': "1.3.6.1.2.1.31.1.1.1.9",
                                'ifOutOctets': "1.3.6.1.2.1.31.1.1.1.10", 
                                'ifOutUcastPkts': "1.3.6.1.2.1.31.1.1.1.11", 
                                'ifOutMulticastPkts': "1.3.6.1.2.1.31.1.1.1.12", 
                                'ifOutBroadcastPkts': "1.3.6.1.2.1.31.1.1.1.13"}

    # A list of interface types we care about
    IF_TYPES = ["6"] 
    
    def get_default_config(self):
        """
        Override SNMPCollector.get_default_config method to provide default_config for the SNMPInterfaceCollector
        """
        default_config = SNMPCollector.get_default_config(self)
        default_config['path'] = 'interface'
        return default_config
    
    def convert_to_mbit(self, value):
        """
        Convert bytes to megabits.
        """
        return ((float(value) / 1024.0 / 1024.0) * 8.0 )

    def convert_to_mbyte(self, value):
        """
        Convert bytes to megabytes.
        """
        return (float(value) / 1024.0 / 1024.0)

    def collect_snmp(self, device, host, port, community):
        """
        Collect SNMP interface data from device
        """
        # Log
        self.log.info("Collecting SNMP interface statistics from: %s" % (device))

        # Initialize Units
        units = {
            'Mbit': self.convert_to_mbit, 
            'Mbyte': self.convert_to_mbyte,
            }

        timestamp = time.time()

        # Define a list of interface indexes
        ifIndexes = []

        # Get Interface Indexes 
        ifIndexOid = '.'.join([self.IF_MIB_INDEX_OID])
        ifIndexData = self.walk(ifIndexOid, host, port, community)
        ifIndexes = [v for v in ifIndexData.values()]

        for ifIndex in ifIndexes:
            # Get Interface Type
            ifTypeOid = '.'.join([self.IF_MIB_TYPE_OID, ifIndex])
            ifTypeData = self.get(ifTypeOid, host, port, community)
            if ifTypeData[ifTypeOid] not in self.IF_TYPES:
                # Skip Interface
                continue
            # Get Interface Name 
            ifNameOid = '.'.join([self.IF_MIB_NAME_OID, ifIndex])
            ifNameData = self.get(ifNameOid, host, port, community)
            ifName=ifNameData[ifNameOid]
            # Remove quotes from string
            ifName = re.sub(r'(\"|\')', '', ifName)

            # Get Gauges
            for gaugeName, gaugeOid in self.IF_MIB_GAUGE_OID_TABLE.items():
                ifGaugeOid = '.'.join([self.IF_MIB_GAUGE_OID_TABLE[gaugeName], ifIndex])
                ifGaugeData = self.get(ifGaugeOid, host, port, community)
                ifGaugeValue = ifGaugeData[ifGaugeOid]
                if not ifGaugeValue:
                    continue
 
                # Get Metric Name and Value
                metricIfDescr = re.sub(r'\W', '_', ifName)
                metricName = '.'.join([metricIfDescr, gaugeName])
                metricValue = int(ifGaugeValue)
                # Get Metric Path
                metricPath = '.'.join(['devices', device, self.config['path'], metricName])
                # Create Metric
                metric = Metric(metricPath, metricValue, None, 0)
                # Publish Metric
                self.publish_metric(metric)
            
            # Get counters (64bit)
            for counterName, counterOid in self.IF_MIB_COUNTER_OID_TABLE.items():
                ifCounterOid = '.'.join([self.IF_MIB_COUNTER_OID_TABLE[counterName], ifIndex])
                ifCounterData = self.get(ifCounterOid, host, port, community)
                ifCounterValue = ifCounterData[ifCounterOid]
                if not ifCounterValue:
                    continue

                # Get Metric Name and Value
                metricIfDescr = re.sub(r'\W', '_', ifName)

                if counterName in ['ifInOctets', 'ifOutOctets']: 
                    for u in units:
                        # Convert Metric  
                        metricName = '.'.join([metricIfDescr, counterName.replace('Octets', u)])
                        metricValue = units[u](int(ifCounterValue))

                        # Get Metric Path
                        metricPath = '.'.join(['devices', device, self.config['path'], metricName])
                        # Create Metric
                        metric = Metric(metricPath, self.derivative(metricPath, metricValue, 18446744073709600000), timestamp, 0)
                        # Publish Metric
                        self.publish_metric(metric)
                else:
                    metricName = '.'.join([metricIfDescr, counterName])
                    metricValue = int(ifCounterValue)

                    # Get Metric Path
                    metricPath = '.'.join(['devices', device, self.config['path'], metricName])
                    # Create Metric
                    metric = Metric(metricPath, self.derivative(metricPath, metricValue, 18446744073709600000), timestamp, 0)
                    # Publish Metric
                    self.publish_metric(metric)

if __name__ == "__main__":

    import diamond.handler
    import logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    # Create Handler
    handler = diamond.handler.NullHandler()

    config={}
    config['server']={}
    config['server']['collectors_config_path']=""
    config['collectors']={}
    config['collectors']['default']={}
    config['collectors']['default']['interval'] = 60
    config['collectors']['default']['splay'] = 0 

    # Load Collector
    collector = SNMPInterfaceCollector(config, [handler])

    # Collect
    #collector.collect_snmp("hardcore", "hardcore.arc", 161, "PebblesSlaghoople")
    #collector.collect_snmp("fls3", "fls3.arc", 161, "PebblesSlaghoople")
    #collector.collect_snmp("routers.jun1", "jun1a.arc", 161, "PebblesSlaghoople")
