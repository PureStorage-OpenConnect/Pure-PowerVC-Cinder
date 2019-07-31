# Copyright (c) 2014 Pure Storage, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
PowerVC Volume driver for Pure Storage FlashArray storage system.

This driver requires Purity version 4.0.0 or later.

Set the following in the cinder.conf file to enable the
PowerVC Pure Storage Fibre Channel Driver along with the required flags:
volume_driver=cinder.volume.drivers.pure_powervc.PureFCDriverPowerVC

"""

from oslo_log import log as logging

from powervc_cinder.volume import discovery_driver
from cinder.volume.drivers.pure import PureBaseVolumeDriver, PureFCDriver
from cinder import utils


LOG = logging.getLogger(__name__)


class PureFCDriverPowerVC(PureFCDriver, discovery_driver.VolumeDiscoveryDriver):
    """OpenStack Volume Driver to support Pure Storage FlashArray in
    an IBM PowerVC environment.

    This version of the driver enables the use of Fibre Channel for
    the underlying storage connectivity with the FlashArray. It fully
    supports the Cinder Fibre Channel Zone Manager.
    """

    VERSION = "6.0.PowerVC"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureFCDriverPowerVC, self).__init__(execute=execute, *args, **kwargs)

    def get_vendor_str(self):
        return 'PURE FlashArray SCSI Disk Device'  # TODO: not sure what's required here.

    def get_volume_info(self, vol_refs, filter_set):
        current_array = self._get_current_array()
        pure_volumes = current_array.list_volumes()  # TODO: Get managed volumes only?
        pure_hosts = current_array.list_hosts()  # This is more efficient than querying individual hosts
        # [u'5001500150015000', u'5001500150015001', u'5001500150015002', u'5001500150015003']
        array_ports = self._get_array_wwns(current_array)
        print 'array_name %s' % current_array.array_name
        LOG.debug("Retrieved volumes on FlashArray %(flash_array)s: %(pure_volumes)s",
                  {"flash_array": current_array.array_name,
                   "pure_volumes": pure_volumes})
        ret = []
        NOT_SUPPORTED_STRING = [u'This volume is not a candidate for management because it is already attached to a '
                                u'virtual machine.  To manage this volume with PowerVC, you must bring the virtual '
                                u'machine under management.  Select to manage the virtual machine that has the volume '
                                u'attached.  The attached volume will be automatically included for management.']
        SUPPORTED_STRING = [u'This volume is a candidate for management because it is not attached to a virtual '
                            u'machine.']
        PURE_REGISTERED_OUI = "624A9370"  # Pure's OUI - ref http://standards.ieee.org/develop/regauth/oui/oui.txt
        # an overview of Network Address Authority (NAA) naming format:
        #   https://bryanchain.com/2016/01/20/breaking-down-an-naa-id-world-wide-name/
        # Network Address Authority (NAA) naming format: https://tools.ietf.org/html/rfc3980#section-5.4
        # Pure volumes expose a SCSI unique ID of the format "naa.<OUI><VolumeSerial>"
        # example: naa.624a9370c7b59c51e9ee20ec00011013, i.e. naa.624a9370 and 24 hex digit volume serial

        # TODO: performance will suffer for large number of volumes!!!!
        for pure_volume in pure_volumes:
            naa_page83 = PURE_REGISTERED_OUI + pure_volume['serial']
            if vol_refs:
                vol_refs_search = [v['pg83NAA'] for v in vol_refs if v['pg83NAA'] == naa_page83]
                if not vol_refs_search:
                    continue  # Skip this volume, not in vol_refs
            print "naa_page83 = %s" % naa_page83
            # Get hosts connected to this volume
            private_connections = current_array.list_volume_private_connections(pure_volume['name'])
            # [{u'host': u'test-h2', u'name': u'test-vol', u'lun': 1, u'size': 5368709120}]
            print 'private_connections: %s' % private_connections
            shared_connections = current_array.list_volume_shared_connections(pure_volume['name'])
            # [{u'host': u'test-h', u'size': 5368709120, u'name': u'test-vol3', u'lun': 254, u'hgroup': u'test-hg'}]
            print 'shared_connections: %s' % shared_connections
            private_connections.extend(shared_connections)
            all_connections = private_connections
            print 'all connections: %s' % all_connections
            itl_list = []
            connect_info = {}
            for pure_connection in all_connections:
                pure_host = next(h for h in pure_hosts if h['name'] == pure_connection['host']) # should be only 1
                # {u'nqn': [], u'iqn': [], u'wwn': [u'0001000100010001', u'0002000200020002'], u'name': u'test-h',
                # u'hgroup': u'test-hg'}
                print "pure_host = %s" % pure_host

                connect_object = {
                    'source_wwn': pure_host['wwn'],
                    'target_lun': pure_connection['lun'],
                    'host': pure_connection['host'],
                    'target_wwn': array_ports
                }
                connect_info[connect_object['host']] = connect_object  # TODO: is the key the host?
                itl_obj = discovery_driver.ITLObject(pure_host['wwn'],
                                                     array_ports,
                                                     pure_connection['lun'],
                                                     vios_host=pure_connection['host'])
                itl_list.append(itl_obj)
            vol_ret = {
                'name': pure_volume['name'],
                'is_mapped': False if not itl_list else True,
                # vol_ret["storage_pool"] = ""  # TODO: what is the storage pool?
                # vol_ret["uuid"]  # TODO: optional, but if we wanted to how would we get it?
                'status': 'available' if not itl_list else 'in use',  # TODO: what are the 'error' conditions?
                'size': self._round_bytes_to_gib(pure_volume['size']),
                'itl_list': itl_list,
                'connection_info': connect_info,
                'pg83NAA': naa_page83,
                'provider_id': pure_volume['name'],
                'restricted_metadata': {
                    'vdisk_id': naa_page83,
                    'vdisk_name': pure_volume['name'],
                    'vdisk_uid': pure_volume['serial'],
                    'naa': naa_page83
                },
                'support': {
                    'status': 'supported' if not itl_list else 'not supported',
                    'reasons': SUPPORTED_STRING if not itl_list else NOT_SUPPORTED_STRING
                }
            }
            ret.append(vol_ret)
        return ret

    def _pre_process_volume_info(self, volume_info, vm_blocking_volumes):
        return  # TODO: validate that we don't need to do anything
