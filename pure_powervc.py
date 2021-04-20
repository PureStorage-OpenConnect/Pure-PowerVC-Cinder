# Copyright (c) 2019 Pure Storage, Inc.
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

from cinder import utils
from cinder.volume.drivers.pure import PureFCDriver
from oslo_log import log as logging
from cinder import context
from cinder.volume import volume_types

from powervc_cinder.volume import discovery_driver
from powervc_cinder.volume.discovery_driver import PORT_LOCATION
from powervc_cinder.volume.discovery_driver import PORT_STATUS
from powervc_cinder.volume.discovery_driver import UNKNOWN_VALUE
from powervc_cinder.db import api as powervc_db_api
from oslo_utils import excutils, importutils
from cinder import exception
from powervc_cinder import exception as powervc_exception
from oslo_utils import units

import json
import six

RESTRICTED_METADATA_VDISK_ID_KEY = "vdisk_id"
RESTRICTED_METADATA_VDISK_UID_KEY = "vdisk_uid"
RESTRICTED_METADATA_VDISK_NAME_KEY = "vdisk_name"

LOG = logging.getLogger(__name__)

def create_restricted_metadata(vol_arg=1):
    """add restricted metadata for a new volume"""
    def wrap(f):
        def decorator(*args, **kwargs):
            args[0]._validate_type(args[vol_arg])
            ret_val = f(*args, **kwargs)
            try:
                args[0]._create_restricted_metadata(args[vol_arg])
            except Exception as ex:
                with excutils.save_and_reraise_exception():
                    LOG.info(_("Create restricted metadata failed. "
                               "Module: %(f_module)s, "
                               "Function: %(f_name)s, "
                               "Volume: %(volume)s, "
                               "Error: %(err)s" %
                               dict(f_module=f.__module__,
                                    f_name=f.__name__,
                                    volume=args[vol_arg]['id'],
                                    err=six.text_type(ex))))
            return ret_val
        return decorator
    return wrap

class PureFCDriverPowerVC(PureFCDriver,
                          discovery_driver.VolumeDiscoveryDriver):
    """OpenStack Driver to support Pure Storage in IBM PowerVC.

    This version of the driver enables the use of Fibre Channel for
    the underlying storage connectivity with the FlashArray. It fully
    supports the Cinder Fibre Channel Zone Manager.
    """

    VERSION = "2.0.PowerVC"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureFCDriverPowerVC, self).__init__(execute=execute,
                                                  *args, **kwargs)

    def get_vendor_str(self):
        # TODO(Pure): not sure what's required here.
        return 'PURE FlashArray SCSI Disk Device'

    def discover_storage_ports(self, details=False, fabric_map=False,
                               all_ports=False):
        available_ports = dict()
        pinfo = {}
        current_array = self._get_current_array()
        ports = current_array.list_ports()
        hardware = current_array.list_hardware()
        for port in ports:
            for count in range(0, len(hardware)):
                if hardware[count]['name'] == port.get('name'):
                    port_speed = self.convert_fc_port_speed(
                            hardware[count]['speed'])
                    pinfo = {
                        'wwpn': port.get('wwn'),
                        'port_name': port.get('name'),
                        PORT_LOCATION: UNKNOWN_VALUE,
                        PORT_STATUS: (
                            'online' if hardware[count]['status'] == 'ok'
                            else hardware[count]['status']),
                        'speed': port_speed}
                    available_ports[pinfo['wwpn']] = pinfo
        if fabric_map:
            self._add_fabric_mapping(available_ports)
        return available_ports

    def convert_fc_port_speed(self, speed):
        if speed == 0:
            return 0
        else:
            speed_byte = float(speed)
            speed_kb = float(1000)
            speed_gb = float(speed_kb ** 3)
            if speed_gb <= speed_byte:
                return '{0:.0f}'.format(speed_byte / speed_gb)      

    def get_volume_info(self, vol_refs, filter_set):
        if vol_refs or filter_set:
            LOG.debug("Filter Set %(filter_set)s "
                      "vol_refs : %(vol_rf)s",
                      {"filter_set": filter_set,
                       "vol_rf": vol_refs})
        current_array = self._get_current_array()
        # TODO(Pure): Get managed volumes only?
        pure_volumes = current_array.list_volumes()
        # This is more efficient than querying individual hosts
        pure_hosts = current_array.list_hosts()
        # [u'5001500150015000', u'5001500150015001',
        #  u'5001500150015002']
        array_ports = self._get_array_wwns(current_array)
        LOG.debug("Retrieved volumes on FlashArray"
                  " %(flash_array)s: %(pure_volumes)s",
                  {"flash_array": current_array.array_name,
                   "pure_volumes": pure_volumes})
        ret = []
        PURE_REGISTERED_OUI = "624A9370"
        # Pure's OUI -ref http://standards.ieee.org/develop/regauth/oui/oui.txt
        # an overview of Network Address Authority (NAA) naming format:
        # Network Address Authority (NAA) naming format:
        #  https://tools.ietf.org/html/rfc3980#section-5.4
        # Pure volumes expose a SCSI unique ID of the
        # format "naa.<OUI><VolumeSerial>"
        # example: naa.624a9370c7b59c51e9ee20ec00011013, i.e. naa.624a9370
        # and 24 hex digit volume serial

        # TODO(Pure): performance will suffer for large number of volumes!!!!
        for pure_volume in pure_volumes:
            naa_page83 = PURE_REGISTERED_OUI + pure_volume['serial']
            vol_refs_search = None
            if vol_refs:
                vol_refs_search = ([v['pg83NAA'] for v in vol_refs
                                    if v['pg83NAA'] == naa_page83])
                if not vol_refs_search:
                    continue  # Skip this volume, not in vol_refs
            # Get hosts connected to this volume
            private_connections = \
                current_array.list_volume_private_connections(
                    pure_volume['name'])
            # [{u'host': u'test-h2', u'name': u'test-vol', u'lun': 1,
            #   u'size': 5368709120}]
            shared_connections = \
                current_array.list_volume_shared_connections(
                    pure_volume['name'])
            # [{u'host': u'test-h', u'size': 5368709120, u'name': u'test-vol3',
            # u'lun': 254, u'hgroup': u'test-hg'}]
            private_connections.extend(shared_connections)
            all_connections = private_connections
            itl_list = []
            connect_info = {}
            for pure_connection in all_connections:
                pure_host = next(h for h in pure_hosts
                                 if h['name'] == pure_connection['host'])
                # should be only 1
                # {u'nqn': [], u'iqn': [], u'wwn': [u'0001000100010001',
                # u'0002000200020002'], u'name': u'test-h',
                # u'hgroup': u'test-hg'}

                connect_object = {
                    'source_wwn': pure_host['wwn'],
                    'target_lun': pure_connection['lun'],
                    'host': pure_connection['host'],
                    'target_wwn': array_ports
                }
                # TODO(Pure): is the key the host?
                connect_info[connect_object['host']] = connect_object
                itl_obj = discovery_driver.ITLObject(
                    pure_host['wwn'], array_ports, pure_connection['lun'],
                    vios_host=pure_connection['host'])
                itl_list.append(itl_obj)
            vol_ret = {
                'name': pure_volume['name'],
                'is_mapped': False if not itl_list else True,
                # TODO(Pure): what is the storage pool?
                # vol_ret["storage_pool"] = ""
                # TODO(Pure): optional, but how would we get it?
                # vol_ret["uuid"]
                # TODO(Pure): what are the 'error' conditions?
                'status': 'available' if not itl_list else 'in use',
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
                }}
            wwpns = list()
            for key in connect_info:
                wwpns.extend(connect_info[key].get('source_wwn'))

            if filter_set is not None:
                lcl_wwpns = set([wwpn.upper() for wwpn in wwpns])
            vol_ret['support'] = {"status": "supported"}
            self._check_volume_status(vol_ret)
            self._check_in_use(vol_ret)
            # If we are not filtering on WWPNs or UID's, or we ARE filtering
            # and there is a match, add this disk to the list to be returned.
            if (filter_set is None or len(filter_set & lcl_wwpns) > 0 or
                    vol_refs_search):
                ret.append(vol_ret)
        return ret

    def _pre_process_volume_info(self, volume_info, vm_blocking_volumes):
        return  # TODO(Pure): validate that we don't need to do anything

    def _add_default_volume_type(self, volume):
        """Adds the default volume type to the volume if the volume does not
        have a volume type yet."""
        ctxt = context.get_admin_context()
        volume_type_id = volume.get('volume_type_id')
        if not volume_type_id:
            volume_type = volume_types.get_default_volume_type()
            if volume_type:
                volume_type_id = volume_type['id']
                volume['volume_type_id'] = volume_type_id
                # Update db to preserve volume_type
                LOG.info(('Adding volume_type_id to volume=%s') % volume)
                self.db.volume_update(ctxt, volume['id'],
                                      {'volume_type_id': volume_type_id})
    
    @create_restricted_metadata()
    def create_cloned_volume(self, tgt_volume, src_volume):
        """
        Overrides the superclass create_cloned_volume.
        Sets default volume type if one is not specified for
        volumes created during VM deployment.
        """
        self._add_default_volume_type(tgt_volume)
        return super(
            PureFCDriverPowerVC,
            self).create_cloned_volume(
            tgt_volume,
            src_volume)

    def _get_snap_name(self, snapshot):
        """Return the name of the snapshot that Purity will use."""
        return "%s.%s" % (self._get_vol_name(snapshot.volume),
                          snapshot["name"].replace('_', '-'))
 
   @create_restricted_metadata()
    def create_volume(self, volume):
        """Overrides the superclass create_volume.

        Sets decorator to call restricted metadata
        """
        model_update = super( PureFCDriverPowerVC,
                              self).create_volume(volume)
        
        return self._model_update(model_update, volume)

    def _create_restricted_metadata(self, volume_obj):
            """create restricted metadata for a volume"""
            vdisk = self.get_volume_info(volume_obj, filter_set)
            if vdisk is not None:
                metadata = {RESTRICTED_METADATA_VDISK_ID_KEY: vdisk['id'],
                            RESTRICTED_METADATA_VDISK_NAME_KEY: vdisk['name'],
                            RESTRICTED_METADATA_VDISK_UID_KEY: vdisk['vdisk_UID']
                            }
                LOG.debug('update_restricted_metadata metadata: %s' % metadata)
                powervc_db_api.volume_restricted_metadata_update_or_create(
                    context.get_admin_context(), volume_obj['id'], metadata)
            else:
                raise powervc_exception.SVCVdiskNotFoundException(
                    self.endpoint_desc, volume_obj['id'],
                    vdisk_id=volume_obj['name'])
                
    def _model_update(self, model_update, volume):
        """add volume wwn to the metadata of the new volume"""
        if not model_update:
            model_update = {}
        meta = self.db.volume_metadata_get(
            context.get_admin_context(), volume['id'])
        model_update['metadata'] = meta if meta else dict()
        attrs = self.get_volume_info(volume)
        model_update['metadata']['volume_wwn'] = attrs['vdisk_UID']
        return model_update     
