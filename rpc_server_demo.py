#!/usr/bin/env python
# ~*~ coding: utf-8 ~*~
#

from __future__ import absolute_import, unicode_literals
import logging
from base import Synapse

logging.basicConfig(level=logging.INFO)

app = Synapse('cmdb', app_id='BSDS')


#@app.rpc_server.callback('asset_list', methods=['GET'])
def asset_list(arg):
    return {'msg': {'name': 'asset_list',
                    'assets': [{'id': 1, 'ip': '172.16.1.2', 'assst_name': 'localhost'},
                               {'id': 2, 'ip': '172.16.1.3', 'asset_name': 'localhost'},
                               ]
                    },
            'failed': False}


if __name__ == '__main__':
    app.rpc_server.callback_map = {
        'asset_list.get': asset_list,
    }
    #app.rpc_server.add_callback_map(asset_list, 'asset_list.get')
    print(app.rpc_server.callback_map)
    app.run(1)
