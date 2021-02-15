# Copyright (C) 2021 OVH SAS
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

import six

from oio import ObjectStorageApi
from oio.common import exceptions
from oio.common.client import ProxyClient
from oio.common.constants import M2_PROP_SHARDING_SHARD_INFO, STRLEN_CID
from oio.common.easy_value import int_value, is_hexa
from oio.common.exceptions import OioException
from oio.common.utils import cid_from_name
from oio.event.beanstalk import Beanstalk


class ContainerSharding(ProxyClient):

    def __init__(self, conf, logger=None, **kwargs):
        super(ContainerSharding, self).__init__(
            conf, request_prefix="/container/sharding", logger=logger,
            **kwargs)

        self.api = ObjectStorageApi(
            self.conf['namespace'], logger=self.logger, **kwargs)

    def _make_params(self, account=None, reference=None, path=None,
                     cid=None, **kwargs):
        if cid:
            params = {'cid': cid}
        else:
            params = {'acct': account, 'ref': reference}
        if path:
            params.update({'path': path})
        return params

    def _format_shard(self, shard, new_shard=False):
        if not isinstance(shard, dict):
            raise ValueError(
                'Expected an object to describe a shard range')
        new_shard = dict()

        shard_index = shard.get('index')
        if shard_index is None:
            raise ValueError('Expected an "index" in the shard range')
        try:
            shard_index = int(shard_index)
        except ValueError:
            raise ValueError('Expected a number for the "index"')
        if shard_index < 0:
            raise ValueError('Expected a positive number for the "index"')
        new_shard['index'] = shard_index

        shard_lower = shard.get('lower')
        if shard_lower is None:
            raise ValueError('Expected a "lower" in the shard range')
        if six.PY3 and isinstance(shard_lower, six.binary_type):
            shard_lower = shard_lower.decode('utf-8')
        elif six.PY2 and isinstance(shard_lower, six.text_type):
            shard_lower = shard_lower.encode('utf-8')
        elif not isinstance(shard_lower, str):
            raise ValueError('Expected a string for the "lower"')
        new_shard['lower'] = shard_lower

        shard_upper = shard.get('upper')
        if shard_upper is None:
            raise ValueError('Expected a "upper" in the shard range')
        if six.PY3 and isinstance(shard_upper, six.binary_type):
            shard_upper = shard_upper.decode('utf-8')
        elif six.PY2 and isinstance(shard_upper, six.text_type):
            shard_upper = shard_upper.encode('utf-8')
        elif not isinstance(shard_upper, str):
            raise ValueError('Expected a string for the "upper"')
        new_shard['upper'] = shard_upper

        if shard_lower != '' and shard_upper != '' \
                and shard_lower >= shard_upper:
            raise ValueError('Expected a "upper" greater the "lower"')

        if not new_shard:
            shard_cid = shard.get('cid')
            if shard_cid is None:
                raise ValueError('Expected a "cid" in the shard range')
            if six.PY3 and isinstance(shard_cid, six.binary_type):
                shard_cid = shard_cid.decode('utf-8')
            elif six.PY2 and isinstance(shard_cid, six.text_type):
                shard_cid = shard_cid.encode('utf-8')
            elif not isinstance(shard_cid, str):
                raise ValueError('Expected a string for the "cid"')
            if not is_hexa(shard_cid, size=STRLEN_CID):
                raise ValueError('Expected a container ID for the "cid"')
            new_shard['cid'] = shard_cid

        return new_shard

    def _format_shards(self, shards, new_shards=False):
        if not isinstance(shards, list):
            raise ValueError('Expected a list of shard ranges')

        if len(shards) < 2:
            raise ValueError('Expected at least 2 shards')

        new_shards = list()
        for shard in shards:
            new_shards.append(self._format_shard(shard, new_shard=new_shards))

        new_shards.sort(key=lambda new_shard: new_shard['index'])
        first_shard_index = new_shards[0]['index']
        last_shard_index = new_shards[-1]['index']
        previous_shard_upper = ''
        for i, new_shard in enumerate(new_shards):
            if new_shard['index'] != i:
                raise ValueError('Missing "index" %d' % i)

            if new_shard['lower'] != previous_shard_upper:
                if i == first_shard_index:
                    raise ValueError(
                        'Expected an empty "lower" for the first shard')
                else:
                    raise ValueError(
                        'Expected the same "lower" as the "upper" '
                        'of the previous shard')
            previous_shard_upper = new_shard['upper']

            if i == last_shard_index and new_shard['upper'] != '':
                raise ValueError(
                    'Expected an empty "upper" for the last shard')

        return new_shards

    def _prepare_sharding(self, account, root_container, **kwargs):
        params = self._make_params(account, reference=root_container, **kwargs)
        resp, body = self._request('POST', '/prepare', params=params, **kwargs)
        if resp.status != 204:
            raise exceptions.from_response(resp, body)
        timestamp = resp.headers.get('x-oio-sharding-timestamp')
        if timestamp:
            timestamp = int_value(timestamp, None)
        if not timestamp:
            raise OioException("Missing timestamp")
        queue_url = resp.headers.get('x-oio-sharding-queue-url')
        if not queue_url:
            raise OioException("Missing queue URL")
        return timestamp, queue_url

    def _create_shard(self, root_account, root_container, timestamp, shard,
                      parent_cid=None, **kwargs):
        root_cid = cid_from_name(root_account, root_container)
        if not parent_cid:
            parent_cid = cid_from_name(root_account, root_container)
        shard_account = '.shards_%s' % (root_account)
        shard_container = '%s-%s-%d-%d' % (
            root_container, parent_cid, timestamp, shard['index'])

        # Create shard container
        shard_info = {
            'root_cid': root_cid,
            'timestamp': timestamp,
            'lower': shard['lower'],
            'upper': shard['upper']
        }
        params = self._make_params(shard_account, reference=shard_container,
                                   **kwargs)
        resp, body = self._request('POST', '/create_shard', params=params,
                                   json=shard_info, **kwargs)
        if resp.status != 204:
            raise exceptions.from_response(resp, body)

        # Fill the shard info with the CID of the shard container
        shard['cid'] = cid_from_name(shard_account, shard_container)

    def _replace_shards(self, account, root_container, shards, **kwargs):
        params = self._make_params(account, reference=root_container, **kwargs)
        resp, body = self._request('POST', '/replace', params=params,
                                   json=shards, **kwargs)
        if resp.status != 204:
            raise exceptions.from_response(resp, body)

    def show_shards(self, root_account, root_container, **kwargs):
        params = self._make_params(root_account, reference=root_container,
                                   **kwargs)
        resp, body = self._request('GET', '/show', params=params, **kwargs)
        if resp.status != 200:
            raise exceptions.from_response(resp, body)
        return body

    def replace_shards(self, root_account, root_container, shards,
                       enable=False):
        shards = self._format_shards(shards, new_shards=True)

        meta = self.api.container_get_properties(root_account, root_container)
        system = meta.get('system', dict())

        if system.get(M2_PROP_SHARDING_SHARD_INFO) is not None:
            raise ValueError('Container is already a shard container')

        old_shards = self.show_shards(root_account, root_container)
        if old_shards:
            # TODO(adu): Already sharded
            raise NotImplementedError()
        elif not enable:
            raise ValueError('Sharding is not enabled for this container')

        try:
            # Prepare the sharding
            timestamp, queue_url = self._prepare_sharding(
                    root_account, root_container)

            # Create the new shards
            for new_shard in shards:
                self._create_shard(root_account, root_container, timestamp,
                                   new_shard)

            root_cid = cid_from_name(root_account, root_container)
            sharding_beanstalkd = Beanstalk.from_url(queue_url)
            sharding_tube = root_cid + '.sharding-' + str(timestamp)
            sharding_beanstalkd.use(sharding_tube)
            sharding_beanstalkd.watch(sharding_tube)
            # while True:
            #     job_id, data = sharding_beanstalkd.reserve(timeout=0)
            #     print(data)
            #     sharding_beanstalkd.delete(job_id)
            sharding_beanstalkd.close()

            # TODO(adu) Lock the root container

            # Apply the new shards
            self._replace_shards(root_account, root_container, shards)
        except Exception:
            # TODO(adu) Abort sharding
            raise
